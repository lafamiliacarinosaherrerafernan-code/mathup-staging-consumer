const crypto=globalThis.crypto;
import { codedError, MemoryOfflineQueue } from "./browser-queue.mjs";
import { draftScope } from './browser-queue.mjs';

export const SUPABASE_STAGING_ADAPTER_STATUS = "PREPARED_NOT_WIRED_TO_PRODUCTION";

const clone = (value) => value == null ? value : structuredClone(value);
const ACCESS_DENIED = new Set(["AUTHENTICATION_REQUIRED", "AUTHORIZATION_DENIED",
  "ENROLLMENT_NOT_OWNED", "ENROLLMENT_NOT_ELIGIBLE", "ENROLLMENT_AMBIGUOUS", "ENROLLMENT_COURSE_MISMATCH"]);
const IDEMPOTENCY_CONFLICTS = ["OPEN_ATTEMPT_ID_CONFLICT", "CLIENT_EVENT_ID_CONFLICT", "ATTEMPT_ID_CONFLICT"];
const REVIEW_REQUIRED = new Set([...IDEMPOTENCY_CONFLICTS, "RPC_IDEMPOTENCY_CONFLICT",
  "CONCURRENCY_ISOLATION_NOT_SUPPORTED", "SESSION_REVISION_CONFLICT"]);
const canQueueOffline = (error) => error.code === "NETWORK_UNAVAILABLE"
  || (error.code === "SUPABASE_RPC_ERROR" && error.details?.postgresCode == null);

function mapRpcError(error) {
  const message = String(error?.message || error?.details || "SUPABASE_RPC_ERROR");
  // SQLSTATE is authoritative for these errors, even without a message or with
  // a conflicting message. Never classify them as offline/retryable transport.
  if (error?.code === "25001") return codedError("CONCURRENCY_ISOLATION_NOT_SUPPORTED", { postgresCode: "25001" });
  if (error?.code === "23505") return codedError(
    IDEMPOTENCY_CONFLICTS.find((item) => message.includes(item)) || "RPC_IDEMPOTENCY_CONFLICT",
    { postgresCode: "23505" });
  const known = [
    "AUTHENTICATION_REQUIRED", "ENROLLMENT_NOT_OWNED", "CONTRACT_REF_NOT_ACCREDITED",
    "ENROLLMENT_NOT_ELIGIBLE", "ENROLLMENT_AMBIGUOUS", "ENROLLMENT_COURSE_MISMATCH",
    "OPEN_ATTEMPT_ID_CONFLICT", "SESSION_NOT_FOUND", "SESSION_NOT_OPEN",
    "SESSION_REVISION_CONFLICT", "PART_NOT_IN_SESSION", "CLIENT_EVENT_ID_CONFLICT",
    "CLIENT_DERIVED_RESULT_FORBIDDEN", "ATTEMPT_ID_CONFLICT", "PINNED_CONTRACT_UNAVAILABLE",
    "INVALID_SELECTED_OPTION", "OPEN_RESPONSE_REQUIRES_MANUAL_REVIEW_ACKNOWLEDGEMENT",
  ];
  const code = known.find((item) => message.includes(item)) || (error?.code === "42501" ? "AUTHORIZATION_DENIED"
    : error?.code === "40001" ? "SESSION_REVISION_CONFLICT" : "SUPABASE_RPC_ERROR");
  return codedError(code, { postgresCode: error?.code ?? null, message });
}

export class SupabaseAnswerContractTransport {
  constructor(client) {
    if (!client?.rpc) throw codedError("SUPABASE_CLIENT_REQUIRED");
    this.client = client;
  }

  async #rpc(name, parameters) {
    const { data, error } = await this.client.rpc(name, parameters);
    if (error) throw mapRpcError(error);
    return data;
  }

  openSession(input) {
    return this.#rpc("open_answer_contract_session", {
      p_open_attempt_id: input.openAttemptId,
      p_enrollment_id: input.enrollmentId ?? null,
      p_contract_refs: input.contractRefs,
    });
  }

  recoverSession({ openAttemptId }) {
    return this.#rpc("get_answer_contract_session", { p_open_attempt_id: openAttemptId });
  }

  syncSessionEvent(input) {
    return this.#rpc("sync_answer_contract_session_event", {
      p_open_attempt_id: input.openAttemptId,
      p_client_event_id: input.clientEventId,
      p_expected_revision: input.expectedRevision,
      p_event_type: input.eventType,
      p_part_id: input.partId,
      p_event_payload: input.payload ?? {},
    });
  }

  completeAttempt(input) {
    const forbidden = ["correct", "scoreAwarded", "points", "progress", "rewards", "evaluationResult"]
      .filter((field) => Object.hasOwn(input || {}, field));
    if (forbidden.length) throw codedError("CLIENT_DERIVED_RESULT_FORBIDDEN", { forbidden });
    return this.#rpc("record_answer_contract_attempt", {
      p_open_attempt_id: input.openAttemptId,
      p_attempt_id: input.attemptId,
      p_part_id: input.partId,
      p_response: input.response,
    });
  }
}

export class AsyncAnswerContractStagingAdapter {
  constructor({ transport, queue = new MemoryOfflineQueue(), idFactory = () => crypto.randomUUID() } = {}) {
    if (!transport) throw codedError("TRANSPORT_REQUIRED");
    this.transport = transport;
    this.queue = queue;
    this.idFactory = idFactory;
    this.cache = new Map();
    this.accessDenied = new Set();
    this.versionDenied = new Set();
    // Same lifetime as this in-memory adapter/queue; not a durable recovery log.
    this.pendingReview = new Map();
    this.closed = false;
  }

  close() { this.closed = true; } // Keep drafts/queue in memory; never flush on close.
  #requireOpen() { if (this.closed) throw codedError("CONSUMER_CLOSED"); }

  async open(input) {
    this.#requireOpen();
    const result = await this.transport.openSession(input);
    this.cache.set(result.session.open_attempt_id || result.session.openAttemptId, clone(result.session));
    return result;
  }

  async recover(openAttemptId) {
    this.#requireOpen();
    try {
      const session = await this.transport.recoverSession({ openAttemptId });
      this.accessDenied.delete(openAttemptId);
      this.cache.set(openAttemptId, clone(session));
      return session;
    } catch (error) { this.#noteDenial(openAttemptId, error); throw error; }
  }

  #noteDenial(openAttemptId, error) {
    if (ACCESS_DENIED.has(error.code)) {
      this.cache.delete(openAttemptId);
      this.accessDenied.add(openAttemptId);
    }
  }

  #requireAccess(openAttemptId) {
    this.#requireOpen();
    if (this.versionDenied.has(openAttemptId)) throw codedError('DRAFT_VERSION_OR_IDENTITY_CONFLICT');
    if (this.accessDenied.has(openAttemptId)) throw codedError("ENROLLMENT_NOT_ELIGIBLE");
  }

  async sync({ openAttemptId, eventType, partId, payload = {}, clientEventId = this.idFactory() }) {
    this.#requireAccess(openAttemptId);
    const cached = this.cache.get(openAttemptId) || await this.recover(openAttemptId);
    const expectedRevision = cached.state_revision ?? cached.stateRevision;
    const request = { openAttemptId, clientEventId, expectedRevision, eventType, partId, payload: clone(payload) };
    if (this.queue.durable) return this.#durableRequest('syncSessionEvent', clientEventId, request);
    try {
      const result = await this.transport.syncSessionEvent(request);
      const session = result.session;
      this.cache.set(openAttemptId, clone(session));
      return { status: result.duplicate ? "DUPLICATE" : "SYNCED", ...result };
    } catch (error) {
      this.#noteDenial(openAttemptId, error);
      if (canQueueOffline(error)) {
        this.queue.push({ clientOperationId: clientEventId, method: "syncSessionEvent", request });
        return { status: "QUEUED_OFFLINE", clientEventId };
      }
      if (error.code === "SESSION_REVISION_CONFLICT") {
        return { status: "CONFLICT_REQUIRES_EXPLICIT_RECONCILIATION", error: error.code };
      }
      throw error;
    }
  }

  async complete(input) {
    this.#requireAccess(input.openAttemptId);
    if (this.queue.durable) return this.#durableRequest('completeAttempt', input.attemptId, clone(input));
    try {
      const result = await this.transport.completeAttempt(input);
      if (result.session) this.cache.set(input.openAttemptId, clone(result.session));
      return { status: result.duplicate ? "DUPLICATE" : "RECORDED", ...result };
    } catch (error) {
      this.#noteDenial(input.openAttemptId, error);
      if (canQueueOffline(error)) {
        this.queue.push({ clientOperationId: input.attemptId, method: "completeAttempt", request: clone(input) });
        return { status: "QUEUED_OFFLINE", clientOperationId: input.attemptId };
      }
      throw error;
    }
  }

  async flush({ clientOperationId, confirmReplay = false } = {}) {
    this.#requireOpen();
    if (this.queue.durable) {
      if (!confirmReplay || !clientOperationId) throw codedError('DRAFT_EXPLICIT_REPLAY_REQUIRED');
      const row = this.queue.peekAll().find(r => r.clientOperationId === clientOperationId);
      if (!row || row.state !== 'OUTCOME_UNKNOWN') throw codedError('DRAFT_REVIEW_REQUIRED_NO_REPLAY');
      // Exact ID/payload/revision only; never amend an uncertain emitted request.
      return [await this.#durableSend(row)];
    }
    const outcomes = [];
    for (const row of this.queue.peekAll()) {
      if (this.pendingReview.has(row.clientOperationId)) {
        outcomes.push({ clientOperationId: row.clientOperationId, status: "BLOCKED_REQUIRES_EXPLICIT_REVIEW",
          error: this.pendingReview.get(row.clientOperationId) });
        continue;
      }
      if (this.accessDenied.has(row.request.openAttemptId)) {
        outcomes.push({ clientOperationId: row.clientOperationId, status: "BLOCKED_ACCESS_REQUIRES_REVIEW" });
        continue;
      }
      try {
        const result = await this.transport[row.method](row.request);
        this.queue.remove(row.clientOperationId);
        outcomes.push({ clientOperationId: row.clientOperationId, status: result.duplicate ? "DUPLICATE_ACKNOWLEDGED" : "SYNCED" });
      } catch (error) {
        this.#noteDenial(row.request.openAttemptId, error);
        if (ACCESS_DENIED.has(error.code)) {
          // Keep the pending draft/operation; denial is not loss of its history.
          outcomes.push({ clientOperationId: row.clientOperationId, status: "BLOCKED_ACCESS_REQUIRES_REVIEW", error: error.code });
        } else if (REVIEW_REQUIRED.has(error.code) || error.details?.postgresCode != null) {
          // Preserve the exact queued payload for review, without dropping it or
          // resending it on subsequent flushes in this adapter session.
          this.pendingReview.set(row.clientOperationId, error.code);
          outcomes.push({ clientOperationId: row.clientOperationId,
            status: error.code === "SESSION_REVISION_CONFLICT" ? "CONFLICT_REQUIRES_EXPLICIT_RECONCILIATION"
              : "BLOCKED_REQUIRES_EXPLICIT_REVIEW", error: error.code });
        } else if (canQueueOffline(error)) {
          outcomes.push({ clientOperationId: row.clientOperationId, status: "REMAINS_QUEUED_OFFLINE" });
        } else {
          this.pendingReview.set(row.clientOperationId, error.code);
          outcomes.push({ clientOperationId: row.clientOperationId, status: "REJECTED_RETAINED_FOR_REVIEW", error: error.code });
        }
      }
    }
    return outcomes;
  }

  bindDraftQueue(queue, identity) {
    this.#requireOpen();
    if (!queue?.durable || JSON.stringify(queue.owner) !== JSON.stringify(identity)) throw codedError('DRAFT_IDENTITY_REQUIRED');
    this.queue = queue; this.draftIdentity = clone(identity);
  }

  async saveLocalDraft(openAttemptId, partId, response) {
    this.#requireOpen();
    if (!this.queue.durable) throw codedError('DRAFT_STORAGE_NOT_ENABLED');
    const scope = draftScope(this.draftIdentity, this.cache.get(openAttemptId), partId);
    await this.queue.saveDraft(scope, response);
    return {status:'SAVED_ON_THIS_DEVICE_NOT_SENT', scope};
  }

  async #durableRequest(method, id, request) {
    const scope = draftScope(this.draftIdentity, this.cache.get(request.openAttemptId), request.partId);
    await this.queue.saveDraft(scope, method === 'syncSessionEvent' ? request.payload.response : request.response);
    await this.queue.prepare(scope, {clientOperationId:id, method, request});
    return this.#durableSend(this.queue.peekAll().find(r => r.clientOperationId === id));
  }

  async #durableSend(row) {
    this.#requireOpen();
    let acknowledged = false;
    try {
      // Before every explicit recovered send. Backend must still gate the write
      // to cover revocation between this read and the following RPC.
      const fresh = await this.recover(row.scope.openAttemptId);
      this.#requireOpen();
      if (JSON.stringify(draftScope(this.draftIdentity,fresh,row.scope.partId)) !== JSON.stringify(row.scope)) {
        this.versionDenied.add(row.scope.openAttemptId);this.cache.delete(row.scope.openAttemptId);
        throw codedError('DRAFT_VERSION_OR_IDENTITY_CONFLICT');
      }
      // CAS checkpoint also prevents a second tab from issuing the same pending
      // operation concurrently. A stale local image is never blindly refreshed.
      await this.queue.mark(row.clientOperationId,'OUTCOME_UNKNOWN');
      this.#requireOpen();
      const result = await this.transport[row.method](clone(row.request));
      this.#requireOpen();
      if (!result?.session || JSON.stringify(draftScope(this.draftIdentity,result.session,row.scope.partId)) !== JSON.stringify(row.scope)
        || (row.method === 'completeAttempt' && result.attempt?.attempt_id !== row.clientOperationId)) throw codedError('DRAFT_ACK_CONTEXT_INVALID');
      acknowledged = true;
      await this.queue.mark(row.clientOperationId,'CONFIRMED_SERVER');
      this.cache.set(row.scope.openAttemptId,clone(result.session));
      return {status:result.duplicate?'DUPLICATE':row.method==='completeAttempt'?'RECORDED':'SYNCED',...result};
    } catch (error) {
      this.#noteDenial(row.scope.openAttemptId,error);
      if (acknowledged) throw codedError('DRAFT_SERVER_ACK_LOCAL_RECORD_FAILED');
      if (this.closed) throw codedError('CONSUMER_CLOSED_OUTCOME_NOT_RECONCILED');
      const state = ACCESS_DENIED.has(error.code) ? 'BLOCKED_ACCESS' : canQueueOffline(error) ? 'OUTCOME_UNKNOWN' : 'REVIEW_REQUIRED';
      // The payload was already persisted. A failed secondary checkpoint must
      // not hide the original error or turn it into success.
      try { await this.queue.mark(row.clientOperationId,state); } catch { /* previous durable state remains conservative */ }
      if (canQueueOffline(error)) return {status:'QUEUED_OFFLINE',clientOperationId:row.clientOperationId,confirmation:'UNKNOWN_NOT_SENT_OR_ACK_LOST'};
      throw error;
    }
  }
}

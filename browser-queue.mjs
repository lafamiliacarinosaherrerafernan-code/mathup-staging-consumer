// Browser-safe implementation of the existing queue contract. No network/Auth.
const copy = value => value == null ? value : structuredClone(value);
export function codedError(code, details = {}) { return Object.assign(new Error(code), { code, details: copy(details) }); }
export class MemoryOfflineQueue {
  constructor(rows = []) { this.rows = copy(rows); }
  push(row) { this.rows.push(copy(row)); }
  peekAll() { return copy(this.rows); }
  remove(id) { this.rows = this.rows.filter(row => row.clientOperationId !== id); }
}

export const DRAFT_PROJECT = 'ooonquzcybeusgwlxmov';
const KEY = 'mathup-isolated-draft-key-v1'; // Key only, never the draft document.
const DB = 'mathup-isolated-drafts-v1';
const MAX_BYTES = 262144, MAX_TEXT = 12000, MAX_ROWS = 64;
const states = ['LOCAL_ONLY', 'OUTCOME_UNKNOWN', 'REVIEW_REQUIRED', 'BLOCKED_ACCESS', 'CONFIRMED_SERVER'];
const need = (yes, code) => { if (!yes) throw codedError(code); };
const equal = (a, b) => JSON.stringify(a) === JSON.stringify(b);
const fields = (v, names) => v && typeof v === 'object' && !Array.isArray(v) && Object.keys(v).every(k => names.includes(k));
const text = v => typeof v === 'string' && v.length > 0 && v.length <= 256;
const uuid = v => typeof v === 'string' && /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(v);
const suspicious = v => /(?:-----BEGIN .*PRIVATE KEY|\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]+\.|\bsb_(?:secret|publishable)_|\b(?:password|contrase[ñn]a|clave|api[_-]?key|access[_-]?token|authorization)\s*[:=])/i.test(v);
export function validateResponse(v) {
  need(fields(v, ['selectedOption', 'learnerResponse', 'manualReviewAcknowledged']), 'DRAFT_RESPONSE_FIELDS');
  if (Object.hasOwn(v, 'selectedOption')) need(Number.isInteger(v.selectedOption) && v.selectedOption >= 0 && v.selectedOption <= 100, 'DRAFT_RESPONSE_TYPE');
  if (Object.hasOwn(v, 'manualReviewAcknowledged')) need(typeof v.manualReviewAcknowledged === 'boolean', 'DRAFT_RESPONSE_TYPE');
  if (Object.hasOwn(v, 'learnerResponse')) need(typeof v.learnerResponse === 'string' && v.learnerResponse.length <= MAX_TEXT && !suspicious(v.learnerResponse), 'DRAFT_TEXT_EXCLUDED_OR_TOO_LONG');
  return copy(v);
}
function ownerValid(v) { return fields(v, ['projectRef', 'userId']) && v.projectRef === DRAFT_PROJECT && uuid(v.userId); }
function scopeValid(v) {
  return fields(v, ['projectRef','userId','enrollmentId','openAttemptId','unit','contractVersion','contractHash','exerciseId','partId'])
    && v.projectRef === DRAFT_PROJECT && uuid(v.userId) && uuid(v.enrollmentId)
    && ['openAttemptId','unit','contractVersion','contractHash','exerciseId','partId'].every(k => text(v[k]));
}
export function draftScope(identity, session, partId) {
  need(ownerValid(identity), 'DRAFT_IDENTITY_REQUIRED');
  const refs = session?.contract_refs?.filter(r => r.partId === partId);
  need(refs?.length === 1, 'DRAFT_REFERENCE_REQUIRED');
  const r = refs[0];
  const scope = { ...identity, enrollmentId: session.enrollment_id, openAttemptId: session.open_attempt_id,
    unit: r.unit, contractVersion: r.contractVersion, contractHash: r.contractHash, exerciseId: r.exerciseId, partId };
  need(scopeValid(scope), 'DRAFT_SCOPE_INVALID');
  return scope;
}
function validateDocument(doc, owner) {
  need(fields(doc, ['format','owner','drafts','operations']) && doc.format === 1 && equal(doc.owner, owner)
    && Array.isArray(doc.drafts) && Array.isArray(doc.operations), 'DRAFT_CORRUPT');
  need(doc.drafts.length<=MAX_ROWS && doc.operations.length<=MAX_ROWS,'DRAFT_CAPACITY');
  const ids = new Set(), scopes = new Set();
  for (const d of doc.drafts) {
    need(fields(d,['scope','response','state']) && scopeValid(d.scope) && d.scope.userId === owner.userId && states.includes(d.state), 'DRAFT_CORRUPT');
    validateResponse(d.response); need(!scopes.has(JSON.stringify(d.scope)), 'DRAFT_CORRUPT'); scopes.add(JSON.stringify(d.scope));
  }
  for (const row of doc.operations) {
    need(fields(row,['scope','clientOperationId','method','request','state']) && scopeValid(row.scope) && row.scope.userId === owner.userId
      && text(row.clientOperationId) && states.includes(row.state) && !ids.has(row.clientOperationId), 'DRAFT_CORRUPT');
    ids.add(row.clientOperationId);
    const q = row.request;
    if (row.method === 'syncSessionEvent') {
      need(fields(q,['openAttemptId','clientEventId','expectedRevision','eventType','partId','payload'])
        && q.clientEventId === row.clientOperationId && Number.isSafeInteger(q.expectedRevision) && q.expectedRevision >= 0
        && q.eventType === 'DRAFT_RESPONSE' && fields(q.payload,['response']) && Object.hasOwn(q.payload,'response'), 'DRAFT_REQUEST_INVALID');
      validateResponse(q.payload.response);
    } else {
      need(row.method === 'completeAttempt' && fields(q,['openAttemptId','attemptId','partId','response']) && q.attemptId === row.clientOperationId, 'DRAFT_REQUEST_INVALID');
      validateResponse(q.response);
    }
    need(q.openAttemptId === row.scope.openAttemptId && q.partId === row.scope.partId, 'DRAFT_REQUEST_SCOPE');
  }
  need(new TextEncoder().encode(JSON.stringify(doc)).length <= MAX_BYTES, 'DRAFT_CAPACITY');
  return doc;
}

// Real IndexedDB CAS: encrypt before beginning the short readwrite transaction.
export class IndexedDraftStore {
  constructor(factory = globalThis.indexedDB) { this.factory = factory; }
  async run(mode, action) {
    need(this.factory, 'DRAFT_STORAGE_UNAVAILABLE');
    const db = await new Promise((resolve, reject) => {
      const r = this.factory.open(DB, 1);
      let abandoned=false;
      r.onupgradeneeded = () => r.result.createObjectStore('vaults');
      r.onsuccess = () => {if(abandoned)r.result.close();else resolve(r.result);};
      r.onerror = () => reject(codedError('DRAFT_STORAGE_UNAVAILABLE'));
      r.onblocked = () => {abandoned=true;reject(codedError('DRAFT_STORAGE_BLOCKED'));};
    });
    try { return await new Promise((resolve, reject) => {
      const tx = db.transaction('vaults', mode), store = tx.objectStore('vaults'); let result, conflict = false;
      tx.oncomplete = () => resolve(result);
      tx.onabort = tx.onerror = () => reject(codedError(conflict ? 'DRAFT_LOCAL_CONFLICT' : 'DRAFT_STORAGE_WRITE_FAILED'));
      action(store, v => { result = v; }, () => { conflict = true; tx.abort(); });
    }); } finally { db.close(); }
  }
  read(id) { return this.run('readonly', (s, done) => { const r = s.get(id); r.onsuccess = () => done(r.result ?? null); }); }
  write(id, expected, row) { return this.run('readwrite', (s, done, conflict) => {
    const r = s.get(id); r.onsuccess = () => {
      if ((r.result?.revision ?? 0) !== expected) return conflict();
      s.put(row, id); done(true);
    };
  }); }
  erase(id) { return this.run('readwrite', (s, done) => { s.delete(id); done(true); }); }
}

const hex = bytes => Array.from(bytes, b => b.toString(16).padStart(2,'0')).join('');
const unhex = s => { need(typeof s === 'string' && /^(?:[0-9a-f]{2})+$/.test(s), 'DRAFT_CORRUPT'); return Uint8Array.from(s.match(/../g), h => parseInt(h,16)); };

export class EncryptedDraftQueue extends MemoryOfflineQueue {
  // Unexpected SDK account change: make the key inaccessible to the new account
  // without silently erasing the previous user's ciphertext. The returned lease
  // must stay in a private closure; restoring requires freshly verified identity.
  // Reload/closing this parked tab loses the memory-only lease (explicit UI limit).
  static parkRecoveryKey(keys=globalThis.sessionStorage,store=new IndexedDraftStore()) {
    let raw; try { raw=keys.getItem(KEY); } catch { throw codedError('DRAFT_KEY_STORAGE_UNAVAILABLE'); }
    if(!raw)return null;
    let record;try{record=JSON.parse(raw);}catch{throw codedError('DRAFT_KEY_CORRUPT');}
    need(ownerValid(record.owner)&&uuid(record.id)&&/^[0-9a-f]{64}$/.test(record.key),'DRAFT_KEY_CORRUPT');
    keys.removeItem(KEY);need(keys.getItem(KEY)===null,'DRAFT_RETIREMENT_UNCONFIRMED_CLOSE_PRIVATE_WINDOW');
    let active=true;
    return Object.freeze({owner:copy(record.owner),
      restore(verifiedOwner){need(active&&equal(verifiedOwner,record.owner),'DRAFT_IDENTITY_REQUIRED');
        need(keys.getItem(KEY)===null,'DRAFT_LOCAL_CONFLICT');keys.setItem(KEY,raw);
        need(keys.getItem(KEY)===raw,'DRAFT_KEY_NOT_SAVED');active=false;raw=null;record=null;},
      async retire({discardLocal=false}={}){need(active&&discardLocal,'DRAFT_DISCARD_CONFIRMATION_REQUIRED');
        await store.erase(record.id);need(await store.read(record.id)===null,'DRAFT_RETIREMENT_UNCONFIRMED_CLOSE_PRIVATE_WINDOW');
        active=false;raw=null;record=null;return {localErased:true,keyErased:true,serverHistoryChanged:false};}
    });
  }
  static hasRecoveryKey(keys=globalThis.sessionStorage) { try{return keys?.getItem(KEY)!=null;}catch{return false;} }
  static async retireStored({discardLocal=false,keys=globalThis.sessionStorage,store=new IndexedDraftStore()}={}) {
    need(discardLocal===true,'DRAFT_DISCARD_CONFIRMATION_REQUIRED');
    let record;try{record=JSON.parse(keys.getItem(KEY));}catch{throw codedError('DRAFT_KEY_CORRUPT');}
    if(!record)return;
    need(uuid(record.id),'DRAFT_KEY_CORRUPT');await store.erase(record.id);
    need(await store.read(record.id)===null,'DRAFT_RETIREMENT_UNCONFIRMED_CLOSE_PRIVATE_WINDOW');
    keys.removeItem(KEY);need(keys.getItem(KEY)===null,'DRAFT_RETIREMENT_UNCONFIRMED_CLOSE_PRIVATE_WINDOW');
  }
  static async open({ owner, consent, keys = globalThis.sessionStorage, store = new IndexedDraftStore(), crypto = globalThis.crypto } = {}) {
    need(consent === true && ownerValid(owner), 'DRAFT_CONSENT_AND_IDENTITY_REQUIRED');
    need(crypto?.subtle && keys, 'DRAFT_STORAGE_UNAVAILABLE');
    const q = new EncryptedDraftQueue();
    Object.assign(q, { owner: copy(owner), keys, store, crypto, closed: false, revision: 0, serial: Promise.resolve() });
    // Only one live consumer of this origin can display a private draft. A
    // duplicated tab must not retain a decrypted view after logout elsewhere.
    if(store instanceof IndexedDraftStore) {
      need(globalThis.navigator?.locks,'DRAFT_BROWSER_LOCK_REQUIRED');
      await new Promise((resolve,reject)=>{
        globalThis.navigator.locks.request('mathup-isolated-draft-view',{ifAvailable:true},async lock=>{
          if(!lock){reject(codedError('DRAFT_OTHER_TAB_ACTIVE'));return;}
          await new Promise(release=>{q.release=release;resolve();});
        }).catch(()=>reject(codedError('DRAFT_BROWSER_LOCK_REQUIRED')));
      });
    }
    try {
    let saved;
    try { saved = keys.getItem(KEY); } catch { throw codedError('DRAFT_KEY_STORAGE_UNAVAILABLE'); }
    if (saved) {
      need(saved.length<=1500,'DRAFT_KEY_CORRUPT');
      try { q.keyRecord = JSON.parse(saved); } catch { throw codedError('DRAFT_KEY_CORRUPT'); }
      need(fields(q.keyRecord,['owner','id','key']) && equal(q.keyRecord.owner, owner) && uuid(q.keyRecord.id), 'DRAFT_OTHER_IDENTITY_CLOSE_REQUIRED');
      need(/^[0-9a-f]{64}$/.test(q.keyRecord.key), 'DRAFT_KEY_CORRUPT');
    } else {
      q.keyRecord = { owner: copy(owner), id: crypto.randomUUID(), key: hex(crypto.getRandomValues(new Uint8Array(32))) };
      try { keys.setItem(KEY, JSON.stringify(q.keyRecord)); need(keys.getItem(KEY) === JSON.stringify(q.keyRecord), 'DRAFT_KEY_NOT_SAVED'); }
      catch { throw codedError('DRAFT_KEY_NOT_SAVED'); }
    }
    q.key = await crypto.subtle.importKey('raw', unhex(q.keyRecord.key), 'AES-GCM', false, ['encrypt','decrypt']);
    q.doc = { format:1, owner:copy(owner), drafts:[], operations:[] };
    const row = await store.read(q.keyRecord.id);
    if (row) {
      try {
        need(fields(row,['revision','iv','cipher']) && Number.isSafeInteger(row.revision) && row.revision > 0
          && typeof row.iv==='string' && row.iv.length===24 && typeof row.cipher==='string'
          && row.cipher.length<=2*(MAX_BYTES+16) && row.cipher.length>=32,'DRAFT_CORRUPT');
        const bytes = await crypto.subtle.decrypt({name:'AES-GCM', iv:unhex(row.iv), additionalData:new TextEncoder().encode(q.keyRecord.id)},q.key,unhex(row.cipher));
        q.doc = validateDocument(JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(bytes)),owner); q.revision = row.revision;
      } catch { throw codedError('DRAFT_CORRUPT_RETAINED'); }
    } else if (saved) throw codedError('DRAFT_RECORD_MISSING_REVIEW_REQUIRED');
    else await q.change(() => {}); // Verify actual durable storage before claiming it.
    q.durable = true;
    return q;
    } catch(e) {q.release?.();throw e;}
  }
  change(mutate) {
    const job = this.serial.then(async () => {
      need(!this.closed && this.key, 'DRAFT_CLOSED');
      need(this.keys.getItem(KEY) === JSON.stringify(this.keyRecord), 'DRAFT_IDENTITY_CHANGED');
      const next = copy(this.doc); mutate(next); validateDocument(next,this.owner);
      const iv = this.crypto.getRandomValues(new Uint8Array(12));
      const cipher = await this.crypto.subtle.encrypt({name:'AES-GCM',iv,additionalData:new TextEncoder().encode(this.keyRecord.id)},this.key,new TextEncoder().encode(JSON.stringify(next)));
      await this.store.write(this.keyRecord.id,this.revision,{revision:this.revision+1,iv:hex(iv),cipher:hex(new Uint8Array(cipher))});
      this.doc = next; this.revision++;
    });
    this.serial = job.catch(() => {}); return job;
  }
  peekAll() { need(!this.closed,'DRAFT_CLOSED'); return copy(this.doc.operations); }
  drafts() { need(!this.closed,'DRAFT_CLOSED'); return copy(this.doc.drafts); }
  async saveDraft(scope,response) {
    return this.change(doc => { const item={scope:copy(scope),response:validateResponse(response),state:'LOCAL_ONLY'};
      const i=doc.drafts.findIndex(d=>equal(d.scope,scope)); if(i<0)doc.drafts.push(item);else doc.drafts[i]=item;
    });
  }
  async prepare(scope, row) {
    return this.change(doc => {
      const old=doc.operations.find(r=>r.clientOperationId===row.clientOperationId);
      need(!old, old && equal(old.request,row.request) ? 'DRAFT_PENDING_EXPLICIT_REPLAY_REQUIRED' : 'DRAFT_ID_CONFLICT');
      need(!doc.operations.some(r=>equal(r.scope,scope)&&r.state!=='CONFIRMED_SERVER'),'DRAFT_PENDING_REVIEW_REQUIRED');
      doc.operations.push({...copy(row),scope:copy(scope),state:'OUTCOME_UNKNOWN'});
    });
  }
  async mark(id,state) { return this.change(doc => { need(states.includes(state),'DRAFT_STATE');
    const row=doc.operations.find(r=>r.clientOperationId===id);need(row,'DRAFT_OPERATION_MISSING');row.state=state;
    const draft=doc.drafts.find(d=>equal(d.scope,row.scope));
    const response=row.method==='syncSessionEvent'?row.request.payload.response:row.request.response;
    if(draft && equal(draft.response,response))draft.state=state;
  }); }
  async push() { throw codedError('DRAFT_PREPARE_BEFORE_NETWORK_REQUIRED'); }
  async remove() { throw codedError('DRAFT_ACK_REQUIRED_NO_DROP'); }
  async retire({discardLocal = false} = {}) {
    need(discardLocal === true, 'DRAFT_DISCARD_CONFIRMATION_REQUIRED');
    this.closed = true; await this.serial;
    const id=this.keyRecord.id; let erased=false,keyErased=false;
    try { await this.store.erase(id); erased=(await this.store.read(id))===null; } catch { /* keep failure separate */ }
    try { this.keys.removeItem(KEY); keyErased=this.keys.getItem(KEY)===null; } catch { /* never claim erasure */ }
    this.doc={format:1,owner:copy(this.owner),drafts:[],operations:[]};this.rows=[];this.key=null;this.keyRecord=null;
    need(erased && keyErased,'DRAFT_RETIREMENT_UNCONFIRMED_CLOSE_PRIVATE_WINDOW');
    this.release?.();
    return {localErased:true,keyErased:true,serverHistoryChanged:false};
  }
  close() { this.closed=true;this.doc=null;this.key=null;this.keyRecord=null;this.release?.(); }
}

(function initializeNativeStudent(){
 let client=null,boundConfiguration=null,logoutReceipt=null;

  const routes = {"/auth/v1/token":["POST"],"/auth/v1/user":["GET"],"/auth/v1/logout":["POST"],"/rest/v1/profiles":["GET"],"/rest/v1/rpc/get_my_enrollment_access":["POST"],"/rest/v1/rpc/claim_app_session":["POST"],"/rest/v1/rpc/heartbeat_app_session":["POST"],"/rest/v1/rpc/release_app_session":["POST"],"/rest/v1/rpc/open_answer_contract_session":["POST"],"/rest/v1/rpc/get_answer_contract_session":["POST"],"/rest/v1/rpc/sync_answer_contract_session_event":["POST"],"/rest/v1/rpc/record_answer_contract_attempt":["POST"]};
  async function restrictedFetch(input, options={}) {
    const url=new URL(typeof input==='string'?input:input.url);
    const method=options.method||'GET';
    if(url.origin!=="https://ooonquzcybeusgwlxmov.supabase.co" || !routes[url.pathname]?.includes(method)) throw new Error('PUBLIC_TRANSPORT_ROUTE_REJECTED');
    if(url.pathname==='/auth/v1/token'&&!['password','refresh_token'].includes(url.searchParams.get('grant_type'))) throw new Error('PUBLIC_TRANSPORT_ROUTE_REJECTED');
    const response=await fetch(input,{...options,signal:options.signal?AbortSignal.any([options.signal,AbortSignal.timeout(8000)]):AbortSignal.timeout(8000)});
    if(url.pathname==='/auth/v1/logout')logoutReceipt=response.status;
    return response;
  }
  async function releaseSession() {return authenticatedRpc('release_app_session',{p_session_token:sessionToken()});}
  async function signOut() {
    logoutReceipt=null;
    const {error}=await getClient().auth.signOut({scope:'global'});
    // This pinned SDK can swallow 401/403. Local SDK removal is not a server ACK.
    return {error:error||(logoutReceipt!==204?new Error('AUTH_LOGOUT_NOT_ACKNOWLEDGED'):null),httpStatus:logoutReceipt};
  }
  function getConfiguration() {
    return {
      url: window.APP_CONFIG?.SUPABASE_URL?.trim() || "",
      publishableKey: window.APP_CONFIG?.SUPABASE_PUBLISHABLE_KEY?.trim() || ""
    };
  }

  function isConfigured() {
    const { url, publishableKey } = getConfiguration();
    return url === "https://ooonquzcybeusgwlxmov.supabase.co"
      && /^sb_publishable_[A-Za-z0-9_-]+$/.test(publishableKey);
  }

  function getClient() {
    if (!isConfigured()) throw new Error("Este candidato sólo admite el staging autorizado.");
    const current = getConfiguration();
    if (client) {
      if (current.url !== boundConfiguration.url || current.publishableKey !== boundConfiguration.publishableKey)
        throw new Error("La configuración ha cambiado. Cierra este consumidor antes de continuar.");
      return client;
    }
    if (!isConfigured()) throw new Error("La configuración pública de Supabase está incompleta.");
    if (!window.supabase?.createClient) throw new Error("No se ha podido cargar la biblioteca de Supabase.");

    const { url, publishableKey } = getConfiguration();
    boundConfiguration = Object.freeze({ url, publishableKey });
    client = window.supabase.createClient(url, publishableKey, {
      global: { fetch: restrictedFetch },
      auth: {
        storageKey: "mathup-native-access-staging-v1",
        autoRefreshToken: false,
        persistSession: true, storage: sessionStorage,
        detectSessionInUrl: false
      }
    });
    return client;
  }

  function sessionToken() {
    const key = "mathup-native-access-staging-app-session-token";
    let token = sessionStorage.getItem(key);
    if (!token) {
      token = crypto.randomUUID();
      sessionStorage.setItem(key, token);
    }
    return token;
  }

  async function signInWithPassword(email, password) {
    const { data, error } = await getClient().auth.signInWithPassword({ email, password });
    if (error) throw error;
    return data;
  }

  async function getSession() {
    const { data, error } = await getClient().auth.getSession();
    if (error) throw error;
    return data.session;
  }

  async function loadStudentContext(user) {
    if (!user?.id) throw accessError("AUTHENTICATION_REQUIRED");
    const { data: profile, error: profileError } = await getClient()
      .from("profiles").select("display_name,age_band,onboarding_completed").eq("user_id", user.id).maybeSingle();
    if (profileError) throw profileError;
    if (!profile?.onboarding_completed) throw accessError("PROFILE_INCOMPLETE");
    // No fallback to metadata, cached enrollment or the browser's clock.
    // The incremental server migration is required before deploying this client.
    const { data, error } = await authenticatedRpc("get_my_enrollment_access", {});
    if (error) throw accessError(accessCode(error));
    const enrollment = data?.enrollment, today = data?.server_date;
    if (data?.eligible !== true || !enrollment?.id || !enrollment?.course_code
        || enrollment.is_current !== true || !validDate(today)
        || !validDate(enrollment.access_starts_at) || !validDate(enrollment.access_ends_at)
        || enrollment.access_starts_at > today || enrollment.access_ends_at < today) {
      throw accessError("ENROLLMENT_NOT_ELIGIBLE");
    }
    return { user, profile, enrollment };
  }

  function validDate(value) {
    return typeof value === "string" && /^\d{4}-\d{2}-\d{2}$/.test(value)
      && Number.isFinite(Date.parse(value + "T00:00:00Z"))
      && new Date(value + "T00:00:00Z").toISOString().slice(0, 10) === value;
  }

  function accessCode(error) {
    return ["AUTHENTICATION_REQUIRED", "ENROLLMENT_AMBIGUOUS", "ENROLLMENT_NOT_ELIGIBLE"]
      .find(code => String(error?.message || "").includes(code)) || "ENROLLMENT_VERIFICATION_REQUIRED";
  }

  function accessError(code) {
    const messages = {
      PROFILE_INCOMPLETE: "Completa tu perfil para solicitar acceso.",
      AUTHENTICATION_REQUIRED: "Debes iniciar sesión.",
      ENROLLMENT_AMBIGUOUS: "Matrícula ambigua: requiere revisión del administrador autorizado.",
      ENROLLMENT_NOT_ELIGIBLE: "No tienes una matrícula vigente. Consulta al administrador; se conserva el historial.",
      ENROLLMENT_VERIFICATION_REQUIRED: "No se ha podido verificar la matrícula. No se ha concedido acceso."
    };
    return Object.assign(new Error(messages[code] || messages.ENROLLMENT_VERIFICATION_REQUIRED), { code });
  }

  async function authenticatedRpc(functionName, parameters) {
    // Never replay a mutation because its acknowledgement or permissions are uncertain.
    return getClient().rpc(functionName, parameters);
  }

  async function claimSession() {
    const { data, error } = await authenticatedRpc("claim_app_session", { p_session_token: sessionToken() });
    if (error) throw error;
    return data === true;
  }

  async function heartbeat() {
    const { data, error } = await authenticatedRpc("heartbeat_app_session", { p_session_token: sessionToken() });
    if (error) throw error;
    return data === true;
  }


window.APP_SUPABASE=Object.freeze({isConfigured,getClient,signInWithPassword,getSession,loadStudentContext,claimSession,heartbeat,releaseSession,signOut});
})();

// Native composition of the existing adapter + encrypted queue. No relay,
// administrative service, contract bank, local grading or automatic replay.
import {SupabaseAnswerContractTransport,AsyncAnswerContractStagingAdapter} from './supabase-staging-adapter.mjs';
import {EncryptedDraftQueue,DRAFT_PROJECT,draftScope,validateResponse,codedError} from './browser-queue.mjs';

const NAV='mathup-native-lab-navigation-v1', TARGET=`https://${DRAFT_PROJECT}.supabase.co`;
const copy=structuredClone;
// JSON object member order is not semantic; types, array order and extra fields
// are. Compare both complete values, never coerce strings/numbers or drop keys.
const canonical=v=>Array.isArray(v)?v.map(canonical):v&&typeof v==='object'?Object.fromEntries(Object.keys(v).sort().map(k=>[k,canonical(v[k])])):v;
const same=(a,b)=>JSON.stringify(canonical(a))===JSON.stringify(canonical(b));
const need=(v,c)=>{if(!v)throw codedError(c)};
const uuid=v=>typeof v==='string'&&/^[0-9a-f-]{36}$/i.test(v);
const RPC=new Set(['open_answer_contract_session','get_answer_contract_session','sync_answer_contract_session_event','record_answer_contract_attempt']);
const messages={
  SIGNED_OUT:'Inicia sesión con la cuenta sintética asignada. No se crearán cuentas aquí.',
  READY:'Acceso comprobado. Recuperar no envía ni reabre una respuesta.',
  LOCAL_ONLY:'Guardado en este dispositivo. No está confirmado por el servidor.',
  LOCAL_SAVING:'Guardando en este dispositivo… Todavía no está confirmado.',
  MEMORY_ONLY:'Sólo en pantalla: puede perderse al recargar. Activa el guardado local si este equipo es privado.',
  CONFIRMED_SERVER:'Confirmado por el servidor. Una respuesta abierta sigue sin calificación automática.',
  LOCKED_IDENTITY:'La cuenta del SDK ha cambiado. Se ha ocultado el trabajo y retirado su clave del almacenamiento de sesión. Vuelve con la cuenta propietaria para recuperarlo, o confirma la retirada local. No recargues: la clave pendiente sólo está en memoria.',
  BLOCKED_ACCESS:'No hay autorización vigente para continuar o enviar. El borrador y el historial se conservan.',
  REVIEW_REQUIRED:'Hay un conflicto o una confirmación no verificable. No se ha sobrescrito ni reenviado nada. Revisa los pendientes.',
  OUTCOME_UNKNOWN:'No se conoce el resultado de la petición. No se reenviará ni se abrirá otra sesión automáticamente.',
  DRAFT_STORAGE_WRITE_FAILED:'No se pudo guardar este cambio. Conserva el texto visible y no cierres sin revisarlo.',
  DRAFT_CAPACITY:'Almacenamiento lleno: este cambio no se ha guardado; los anteriores se conservan.',
  DRAFT_CORRUPT_RETAINED:'El borrador está dañado o no se puede descifrar. Se conserva, pero no se ha recuperado ni enviado.',
  DRAFT_TEXT_EXCLUDED_OR_TOO_LONG:'Texto no guardado: demasiado largo o con un indicio de secreto. No incluyas credenciales ni datos personales.',
  DRAFT_OTHER_TAB_ACTIVE:'Otra pestaña mantiene un borrador abierto. Vuelve a ella; aquí no se muestran sus datos.',
  VERSION_CONFLICT:'La versión o la identidad no coincide. El borrador queda separado y no se copiará ni enviará.',
  CLOSE_UNCONFIRMED:'Vista cerrada, pero algún paso del cierre no se confirmó. No cambies de cuenta ni compartas esta pestaña. No se reenviará el cierre automáticamente.',
  CLOSED:'Copias locales retiradas y cierre Auth confirmado. No se ha borrado el historial del servidor; no equivale a invalidar todos los JWT antiguos.',
  CONFIG_REQUIRED:'Paquete preparado sin clave pública ni identidades habilitadas. Falta la configuración no sensible autorizada del ensayo.',
};
const friendly=c=>messages[c]||(/ENROLLMENT|AUTHENTICATION|AUTHORIZATION/.test(c)?messages.BLOCKED_ACCESS:/CONFLICT|ISOLATION|23505|25001/.test(c)?messages.REVIEW_REQUIRED:/CORRUPT|MISSING_RECORD/.test(c)?messages.DRAFT_CORRUPT_RETAINED:'No se completó la acción. Conserva el trabajo y revisa el estado; no se ha reintentado.');
function el(tag,text,attrs={}){const n=document.createElement(tag);if(text)n.textContent=text;for(const[k,v]of Object.entries(attrs))n.setAttribute(k,v);return n}
function button(text,id,fn){const b=el('button',text,{type:'button',id});b.addEventListener('click',fn);return b}
function noPrivate(v){if(!v||typeof v!=='object')return;for(const[k,x]of Object.entries(v)){need(!['solution','correct','grading_snapshot','gradingSnapshot','access_token','refresh_token'].includes(k),'PRIVATE_PRESENTATION_REJECTED');noPrivate(x)}}

export class NativeConsumer {
  #queue=null;#parked=null;#memoryPark=null;#adapter=null;#ctx=null;#session=null;#target=null;#epoch=0;#sdkId=null;
  #busy=false;#closing=false;#foreignDuringClose=false;#pending=new Set();#write=Promise.resolve();#local={};#conflict=null;#timer=null;#snapshot=null;#unknownOpen=false;#openId=null;
  constructor(api,config,targets){this.api=api;this.config=config;this.targets=targets;this.root=document.getElementById('app');this.phase='SIGNED_OUT';this.closure={local:'NOT_STARTED',release:'NOT_STARTED',auth:'NOT_STARTED'};}
  async start(){
    need(this.config.project===DRAFT_PROJECT&&this.config.url===TARGET,'DESTINATION_NOT_ALLOWED');
    need(Array.isArray(this.config.allowedUsers)&&[0,2].includes(this.config.allowedUsers.length)&&new Set(this.config.allowedUsers).size===this.config.allowedUsers.length&&this.config.allowedUsers.every(uuid),'CONFIG_INVALID');
    if(!this.api.isConfigured()||!this.config.allowedUsers.length){this.status('CONFIG_REQUIRED');return;}
    // Callback remains synchronous: no SDK await under its auth lock.
    this.api.getClient().auth.onAuthStateChange((_event,s)=>this.changed(s?.user?.id||null));
    this.loginView();
    const {data,error}=await this.api.getClient().auth.getSession();
    if(!error&&data.session?.user)await this.run(()=>this.enter(data.session.user.id));
    addEventListener('beforeunload',e=>{if(this.#parked||this.#memoryPark||this.unsaved){e.preventDefault();e.returnValue=''}});
    addEventListener('pagehide',()=>{this.#adapter?.close();this.#queue?.close();clearInterval(this.#timer)});
    addEventListener('pageshow',e=>{if(e.persisted)location.reload()});
  }
  status(code){this.phase=code;const n=document.getElementById('native-status');n.textContent=friendly(code);n.dataset.state=code;}
  diagnostic(e,stage){const code=/^[A-Z0-9_]{1,80}$/.test(e?.code||'')?e.code:'LOCAL_ACTION_FAILED';document.getElementById('native-status').dataset.check=code;document.getElementById('native-status').dataset.stage=stage;return code}
  changed(id){
    if(id===this.#sdkId)return;
    const previous=this.#sdkId;this.#sdkId=id;
    if(this.#closing){if(id&&id!==this.#ctx?.user.id)this.#foreignDuringClose=true;return;}
    if(previous||this.#ctx||this.#queue){
      if(this.unsaved&&this.#ctx&&this.#session)this.#memoryPark={owner:this.#ctx.user.id,openId:this.#openId,response:copy(this.#local)};
      this.#epoch++;this.#adapter?.close();this.#queue?.close();this.#adapter=null;this.#queue=null;clearInterval(this.#timer);
      this.#local={};this.#session=null;this.#ctx=null;this.#conflict=null;this.root.replaceChildren();
      try{this.#parked??=EncryptedDraftQueue.parkRecoveryKey();this.loginView();this.status('LOCKED_IDENTITY');}
      catch(e){this.phase='CLOSE_UNCONFIRMED';this.diagnostic(e,'identity_change');this.status('CLOSE_UNCONFIRMED');}
    }
  }
  guard(g){need(!this.#closing&&g===this.#epoch,'AUTH_CONTEXT_CHANGED');}
  async tracked(operation){
    // PostgREST builders are re-executable thenables. Store one assimilated
    // Promise: awaiting the drain must not execute that builder a second time.
    const p=Promise.resolve(operation);this.#pending.add(p);try{return await p}finally{this.#pending.delete(p)}
  }
  async run(fn){if(this.#busy||this.#closing)return;this.#busy=true;this.buttons(true);try{return await fn()}catch(e){if(this.#closing||(!this.#ctx&&e?.code==='CONSUMER_CLOSED_OUTCOME_NOT_RECONCILED'))return;const c=this.diagnostic(e,'action');if(c!=='AUTH_CONTEXT_CHANGED')this.status(friendly(c)===messages.BLOCKED_ACCESS?'BLOCKED_ACCESS':c);}
    finally{this.#busy=false;if(!this.#closing)this.buttons(false)}}
  buttons(disabled){
    for(const b of this.root.querySelectorAll('button[data-work]'))b.disabled=disabled||(b.id==='draft-enable'&&!!this.#queue)||(['save','submit'].includes(b.id)&&this.#session?.status!=='OPEN');
    // Keep the payload visible but immutable while its receipt is unresolved;
    // an ACK for an old text must never label a newly typed text as confirmed.
    for(const input of this.root.querySelectorAll('#draft-text,[id^="choice-"]'))input.disabled=disabled||this.#session?.status!=='OPEN';
  }
  async fresh(g){
    this.guard(g);const {data,error}=await this.tracked(this.api.getClient().auth.getUser());this.guard(g);
    need(!error&&data.user?.id===this.#sdkId&&this.config.allowedUsers.includes(data.user.id),'AUTHENTICATION_REQUIRED');
    if(this.#ctx)need(data.user.id===this.#ctx.user.id,'AUTH_CONTEXT_CHANGED');
    const ctx=await this.tracked(this.api.loadStudentContext(data.user));this.guard(g);
    if(this.#ctx)need(ctx.enrollment.id===this.#ctx.enrollment.id,'ENROLLMENT_NOT_ELIGIBLE');
    return ctx;
  }
  loginView(){
    this.root.replaceChildren();const form=el('form');form.id='native-login';
    const email=el('input','',{id:'email',type:'email',autocomplete:'username',required:''});const pwd=el('input','',{id:'password',type:'password',autocomplete:'current-password',required:''});
    form.append(el('h2','Acceso al ensayo'),el('label','Correo',{for:'email'}),email,el('label','Contraseña',{for:'password'}),pwd);
    const submit=el('button','Entrar',{type:'submit','data-work':''});form.append(submit);
    form.onsubmit=e=>{e.preventDefault();if(this.#closing)return;const mail=email.value,password=pwd.value;pwd.value='';this.run(async()=>{
      const g=this.#epoch;const {user}=await this.tracked(this.api.signInWithPassword(mail,password));
      // SIGNED_IN may invalidate a former identity; do not accept its old work.
      need(user?.id===this.#sdkId,'AUTH_CONTEXT_CHANGED');await this.enter(user.id);
    })};this.root.append(form);
    if(this.#parked||this.#memoryPark)this.root.append(button('Retirar copias locales sin recuperarlas','parked-retire',()=>this.askClose()));
    this.status('SIGNED_OUT');
  }
  async enter(id){
    need(!this.#closing,'CLOSING');const g=++this.#epoch;await this.#write.catch(()=>{});this.guard(g);
    this.#ctx=null;const ctx=await this.fresh(g);need(ctx.user.id===id,'AUTH_CONTEXT_CHANGED');
    if(this.#memoryPark)need(this.#memoryPark.owner===id,'DRAFT_OTHER_IDENTITY_CLOSE_REQUIRED');
    if(this.#parked){need(this.#parked.owner.userId===id,'DRAFT_OTHER_IDENTITY_CLOSE_REQUIRED');this.#parked.restore({projectRef:DRAFT_PROJECT,userId:id});this.#parked=null;}
    need(await this.tracked(this.api.claimSession()),'APP_SESSION_NOT_CLAIMED');this.guard(g);this.#ctx=ctx;
    this.menu();this.status('READY');
    clearInterval(this.#timer);this.#timer=setInterval(()=>{if(this.#busy||this.#closing)return;this.run(async()=>{const h=this.#epoch;need(await this.tracked(this.api.heartbeat()),'ENROLLMENT_NOT_ELIGIBLE');this.guard(h);await this.fresh(h);})},45000);
  }
  menu(){
    this.root.replaceChildren(el('h2','Dos LAB sintéticos · sin efectos de puntuación'));
    for(const t of this.targets){const b=button(t.label,'open-'+t.partId,()=>this.run(()=>this.open(t)));b.dataset.work='';this.root.append(b)}
    const nav=this.readNav();if(nav&&nav.owner===this.#ctx.user.id){this.root.append(button('Recuperar el ejercicio anterior sin enviar','recover',()=>this.run(()=>this.recover(nav))))}
    this.root.append(button('Cerrar sesión y retirar copias locales','logout',()=>this.askClose()));
  }
  readNav(){try{const n=JSON.parse(sessionStorage.getItem(NAV));if(!n)return null;need(Object.keys(n).sort().join(',')==='openId,owner,partId,snapshot', 'NAV_INVALID');need(uuid(n.owner)&&typeof n.openId==='string'&&n.openId.length<=256&&this.targets.some(t=>t.partId===n.partId)&&typeof n.snapshot==='string','NAV_INVALID');return n}catch{return null}}
  adapter(g){
    const rpc=async(name,args)=>{
      this.guard(g);need(RPC.has(name),'RPC_NOT_ALLOWED');
      const r=await this.tracked(this.api.getClient().rpc(name,args));this.guard(g);
      // The pinned SDK represents a failed fetch (not an HTTP 500) with status 0,
      // data null and an empty SQLSTATE. Preserve uncertainty, never success.
      if(r.status===0&&r.data===null&&r.error?.code===''&&r.statusText==='')return {...r,error:{code:null,message:'LOCAL_TRANSPORT_UNCONFIRMED'}};
      if(!r.error){const s=name==='get_answer_contract_session'?r.data:r.data?.session;this.checkSession(s,this.#target,args.p_open_attempt_id);
        if(name==='sync_answer_contract_session_event'){
          need(Number.isSafeInteger(args.p_expected_revision)&&s.state_revision===args.p_expected_revision+1&&same(s.response_state.draftResponses?.[args.p_part_id],args.p_event_payload.response),'DRAFT_ACK_CONTEXT_INVALID');
        }
        if(name==='record_answer_contract_attempt'){
          const a=r.data.attempt,t=this.#target;need(a?.attempt_id===args.p_attempt_id&&a.session_id===s.id&&a.part_id===t.partId&&a.contract_version===t.contractVersion&&a.contract_hash===t.contractHash&&same(a.response_payload,args.p_response)&&a.functional_score_awarded===0&&s.status==='COMPLETED','DRAFT_ACK_CONTEXT_INVALID');
          if(t.responseMode==='OPEN_RESPONSE')need(a.correct===null,'AUTOMATIC_GRADE_FORBIDDEN');
        }
      }return r;
    };
    return new AsyncAnswerContractStagingAdapter({transport:new SupabaseAnswerContractTransport({rpc})});
  }
  checkSession(s,t,id){
    const ref=s?.contract_refs?.[0],p=s?.question_snapshot?.parts?.[0];
    need(s&&uuid(s.id)&&s.open_attempt_id===id&&s.enrollment_id===this.#ctx.enrollment.id&&s.course_code===this.#ctx.enrollment.course_code&&['OPEN','COMPLETED'].includes(s.status)&&Number.isSafeInteger(s.state_revision)&&s.state_revision>=0,'SESSION_CONTEXT_INVALID');
    need(s.contract_refs.length===1&&s.question_snapshot.parts.length===1&&t&&['unit','contractVersion','contractHash','exerciseId','partId','responseMode'].every(k=>ref[k]===t[k])&&['unit','contractVersion','contractHash','exerciseId','partId'].every(k=>p[k]===t[k]),'VERSION_CONFLICT');
    need(typeof s.question_snapshot_hash==='string'&&/^[a-f0-9]{64}$/i.test(s.question_snapshot_hash),'SNAPSHOT_INVALID');
    if(this.#snapshot)need(this.#snapshot===s.question_snapshot_hash,'VERSION_CONFLICT');
    noPrivate(s.question_snapshot);need(p.presentation?.responseMode===t.responseMode&&typeof p.presentation.statement?.text==='string'&&typeof p.presentation.part?.text==='string'&&Array.isArray(p.presentation.options),'PUBLIC_PRESENTATION_INVALID');
    need((t.responseMode==='OPEN_RESPONSE'&&p.presentation.options.length===0)||(t.responseMode==='SINGLE_CHOICE'&&p.presentation.options.length===2),'PUBLIC_PRESENTATION_INVALID');
    return s;
  }
  async open(t){
    need(!this.#session&&!this.#unknownOpen&&!this.#queue&&!EncryptedDraftQueue.hasRecoveryKey(),'RECOVER_OR_CLOSE_FIRST');const g=this.#epoch;await this.fresh(g);
    this.#target=t;this.#snapshot=null;this.#adapter=this.adapter(g);this.#openId=crypto.randomUUID();this.#unknownOpen=true;
    // Retain uncertain ID before issuing, never open another to compensate.
    sessionStorage.setItem(NAV,JSON.stringify({owner:this.#ctx.user.id,partId:t.partId,openId:this.#openId,snapshot:''}));
    try{const r=await this.#adapter.open({openAttemptId:this.#openId,enrollmentId:this.#ctx.enrollment.id,contractRefs:[{unit:t.unit,contractVersion:t.contractVersion,contractHash:t.contractHash}]});this.guard(g);this.#unknownOpen=false;await this.show(r.session)}
    catch(e){if(g===this.#epoch&&!this.#closing)this.status('OUTCOME_UNKNOWN');throw e}
  }
  async recover(n=this.readNav()){
    need(n&&n.owner===this.#ctx.user.id,'AUTHENTICATION_REQUIRED');const g=this.#epoch;await this.fresh(g);
    this.#target=this.targets.find(t=>t.partId===n.partId);this.#snapshot=n.snapshot||null;this.#openId=n.openId;
    this.#adapter?.close();this.#adapter=this.adapter(g);const s=await this.#adapter.recover(n.openId);this.guard(g);this.#unknownOpen=false;
    await this.show(s);if(EncryptedDraftQueue.hasRecoveryKey())await this.enableRecovery();
    if(this.#memoryPark?.owner===this.#ctx.user.id&&this.#memoryPark.openId===n.openId){this.#conflict=copy(this.#memoryPark.response);this.status('REVIEW_REQUIRED');this.pending();}
  }
  async show(s){
    this.#session=copy(s);this.#snapshot=s.question_snapshot_hash;this.#local=copy(s.response_state.draftResponses?.[this.#target.partId]||{});
    sessionStorage.setItem(NAV,JSON.stringify({owner:this.#ctx.user.id,partId:this.#target.partId,openId:s.open_attempt_id,snapshot:this.#snapshot}));
    this.root.replaceChildren();const p=s.question_snapshot.parts[0].presentation;
    this.root.append(el('h2',p.statement.text),el('p',p.part.text));
    const panel=el('section','',{id:'staging-drafts'});panel.append(el('h3','Tu borrador'));
    const enable=button('Activar guardado en este dispositivo','draft-enable',()=>this.run(()=>this.enableRecovery()));enable.dataset.work='';panel.append(enable);
    panel.append(el('p','Sólo esta pestaña y este origen. No se garantiza recuperar tras cerrar el navegador, perder la clave o cambiar de dispositivo. No escribas secretos ni datos personales.'));
    if(this.#target.responseMode==='OPEN_RESPONSE'){
      const area=el('textarea','',{id:'draft-text',rows:'6',maxlength:'12000'});area.value=this.#local.learnerResponse||'';area.disabled=s.status!=='OPEN';
      panel.append(el('label','Tu respuesta abierta (sin calificación automática)',{for:'draft-text'}),area);
      area.oninput=()=>this.edit({learnerResponse:area.value});
    }else{for(const [i,option]of p.options.entries()){const b=button(option,'choice-'+i,()=>this.edit({selectedOption:i}));b.disabled=s.status!=='OPEN';panel.append(b)}}
    for(const[label,id,fn]of [['Guardar borrador en servidor','save',()=>this.save(false)],['Enviar respuesta','submit',()=>this.save(true)],['Revisar pendientes','review',()=>this.pending()]]){const b=button(label,id,()=>this.run(fn));b.dataset.work='';if(s.status!=='OPEN'&&id!=='review')b.disabled=true;panel.append(b)}
    panel.append(el('div','',{id:'pending'}));this.root.append(panel,button('Cerrar sesión y retirar copias locales','logout',()=>this.askClose()));this.status('READY');
  }
  edit(response){if(this.#closing||!this.#session||this.#session.status!=='OPEN')return;this.#local=copy(response);this.unsaved=true;
    if(this.#conflict){this.status('REVIEW_REQUIRED');return}
    if(!this.#queue){this.status('MEMORY_ONLY');return}
    const g=this.#epoch,adapter=this.#adapter;this.status('LOCAL_SAVING');
    this.#write=adapter.saveLocalDraft(this.#openId,this.#target.partId,response).then(()=>{this.guard(g);this.unsaved=false;this.status('LOCAL_ONLY')}).catch(e=>{if(g===this.#epoch)this.status(this.diagnostic(e,'local_save'))});
  }
  async enableRecovery(){
    const g=this.#epoch;if(this.#queue)return;await this.fresh(g);
    const q=await EncryptedDraftQueue.open({owner:{projectRef:DRAFT_PROJECT,userId:this.#ctx.user.id},consent:true});
    try{this.guard(g)}catch(e){q.close();throw e}this.#queue=q;this.#adapter.bindDraftQueue(q,q.owner);
    const scope=draftScope(q.owner,this.#session,this.#target.partId),d=q.drafts().find(x=>same(x.scope,scope));
    if(d&&!same(d.response,this.#local))this.#conflict=copy(d.response);
    const b=document.getElementById('draft-enable');b.textContent='Guardado local activado';b.disabled=true;
    if(d){this.status(this.#conflict?'REVIEW_REQUIRED':d.state);this.pending();}else if(this.unsaved){this.edit(this.#local);await this.#write;}else this.status('READY');
  }
  async save(submit){
    need(!this.#conflict,'DRAFT_LOCAL_TEXT_CONFLICT');
    const g=this.#epoch;await this.#write;need(!this.unsaved||!this.#queue,'DRAFT_STORAGE_WRITE_FAILED');await this.fresh(g);need(this.#session?.status==='OPEN','SESSION_NOT_OPEN');
    const fresh=await this.#adapter.recover(this.#openId);this.guard(g);this.#session=copy(fresh);need(fresh.status==='OPEN','SESSION_NOT_OPEN');
    const response=validateResponse(copy(this.#local));if(submit&&this.#target.responseMode==='OPEN_RESPONSE')response.manualReviewAcknowledged=true;
    const r=submit?await this.#adapter.complete({openAttemptId:this.#openId,attemptId:`${this.#openId}:submitted`,partId:this.#target.partId,response}):await this.#adapter.sync({openAttemptId:this.#openId,eventType:'DRAFT_RESPONSE',partId:this.#target.partId,payload:{response}});
    this.guard(g);if(r.status==='QUEUED_OFFLINE'){this.status('OUTCOME_UNKNOWN');return}
    need(r.session,'DRAFT_ACK_CONTEXT_INVALID');this.#session=copy(r.session);this.unsaved=false;this.status('CONFIRMED_SERVER');
    if(submit){for(const x of this.root.querySelectorAll('#save,#submit,#draft-text,[id^="choice-"]'))x.disabled=true;this.root.append(el('p','Respuesta registrada. Sin puntos, progreso ni calificación automática de abiertas.',{id:'result'}))}
  }
  pending(){const box=document.getElementById('pending');if(!box)return;box.replaceChildren(el('h3','Revisión de pendientes'));
    if(this.#conflict){box.append(el('p','Hay un borrador guardado distinto del visible. Ninguno se ha enviado automáticamente.'),el('pre',JSON.stringify(this.#conflict)));
      for(const useSaved of [true,false])box.append(button(useSaved?'Recuperar guardado sin enviar':'Conservar texto visible',useSaved?'choose-saved':'choose-visible',()=>this.run(async()=>{
        const g=this.#epoch;await this.fresh(g);const v=copy(useSaved?this.#conflict:this.#local);if(this.#queue)await this.#adapter.saveLocalDraft(this.#openId,this.#target.partId,v);this.guard(g);this.#local=v;this.#conflict=null;this.#memoryPark=null;this.unsaved=!this.#queue;
        const area=document.getElementById('draft-text');if(area)area.value=v.learnerResponse||'';this.status(this.#queue?'LOCAL_ONLY':'MEMORY_ONLY');this.pending();
      })))}
    if(!this.#queue){box.append(el('p','No está activado el guardado cifrado local.'));return}
    for(const row of this.#queue.peekAll().filter(r=>r.scope.openAttemptId===this.#openId&&r.scope.contractHash===this.#target.contractHash)){box.append(el('p',friendly(row.state)),el('pre',JSON.stringify(row.method==='completeAttempt'?row.request.response:row.request.payload.response)));
      if(row.state==='OUTCOME_UNKNOWN')box.append(button('Comprobar acceso y reenviar exactamente','replay',()=>this.run(async()=>{
        const g=this.#epoch;await this.fresh(g);const [r]=await this.#adapter.flush({clientOperationId:row.clientOperationId,confirmReplay:true});this.guard(g);need(r?.session,'DRAFT_ACK_CONTEXT_INVALID');this.#session=copy(r.session);this.status('CONFIRMED_SERVER');this.pending();
      })))}
    for(const d of this.#queue.drafts())if(d.scope.contractHash!==this.#target.contractHash||d.scope.openAttemptId!==this.#openId)box.append(el('p','Hay un borrador de otro apartado, versión o sesión. Se conserva separado.'));
  }
  askClose(){if(this.#closing)return;const d=el('dialog','',{id:'close-dialog'});d.append(el('h2','¿Retirar las copias locales y cerrar?'),el('p','Incluye trabajo no confirmado. No se envía nada al cerrar ni se borra historial del servidor. Puedes cancelar para revisarlo.'));
    const cancel=button('Cancelar y volver','close-cancel',()=>d.close());const yes=button('Retirar copias y cerrar','close-confirm',()=>{d.close();this.close()});d.append(cancel,yes);d.onclose=()=>d.remove();document.body.append(d);d.showModal();cancel.focus();}
  async close(){
    if(this.#closing)return;this.#closing=true;this.#epoch++;clearInterval(this.#timer);this.#adapter?.close();this.root.replaceChildren();this.status('CLOSING');
    const id=this.#ctx?.user.id||this.#sdkId;this.#foreignDuringClose=false;
    try{
      await Promise.race([Promise.allSettled([...this.#pending,this.#write]),new Promise((_,reject)=>setTimeout(()=>reject(codedError('CLOSE_DRAIN_UNCONFIRMED')),8500))]);
      if(this.#queue)await this.#queue.retire({discardLocal:true});else if(this.#parked)await this.#parked.retire({discardLocal:true});else await EncryptedDraftQueue.retireStored({discardLocal:true});
      this.#queue=null;this.#parked=null;this.#memoryPark=null;sessionStorage.removeItem(NAV);need(sessionStorage.getItem(NAV)===null,'CLOSE_LOCAL_UNCONFIRMED');this.closure.local='CONFIRMED';
      need(!this.#foreignDuringClose&&this.#sdkId===id,'CLOSE_IDENTITY_CHANGED');
      try{const r=await this.api.releaseSession();this.closure.release=r.error?'NOT_CONFIRMED':'ACKNOWLEDGED';}catch{this.closure.release='NOT_CONFIRMED'}
      need(!this.#foreignDuringClose&&this.#sdkId===id,'CLOSE_IDENTITY_CHANGED');
      const {error}=await this.api.signOut();this.closure.auth=error||this.#foreignDuringClose?'NOT_CONFIRMED':'ACKNOWLEDGED';
      need(!error&&!this.#foreignDuringClose,'CLOSE_AUTH_UNCONFIRMED');
      this.#ctx=null;this.#session=null;this.#local={};this.#adapter=null;this.#snapshot=null;this.#unknownOpen=false;this.unsaved=false;this.#closing=false;this.loginView();this.status('CLOSED');
    }catch(e){this.diagnostic(e,'close');this.status('CLOSE_UNCONFIRMED');try{this.#parked??=EncryptedDraftQueue.parkRecoveryKey();this.#queue?.close();this.#queue=null;}catch{} }
    const summary=el('p',`Retirada local: ${this.closure.local}. Release: ${this.closure.release}. Auth: ${this.closure.auth}.`,{id:'close-results'});this.root.append(summary);
  }
}

try{
  const response=await fetch('./public-config.json',{cache:'no-store'});need(response.ok,'CONFIG_UNAVAILABLE');const config=await response.json();
  window.APP_CONFIG=Object.freeze({SUPABASE_URL:config.url,SUPABASE_PUBLISHABLE_KEY:config.publishableKey});
  const targets=await (await fetch('./lab-targets.json',{cache:'no-store'})).json();
  const app=new NativeConsumer(window.APP_SUPABASE,config,targets);await app.start();
}catch{
  const n=document.getElementById('native-status');n.textContent=messages.CONFIG_REQUIRED;n.dataset.state='CONFIG_REQUIRED';n.dataset.stage='bootstrap';
}

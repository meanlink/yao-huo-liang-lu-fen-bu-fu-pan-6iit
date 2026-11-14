/** @typedef {{id:number,name:string,source:string,rows:number,checksum:string}} Dataset */
/** @typedef {{id:number,name:string,dataset_id:number,status:string,result:Object,approved:boolean}} AnalysisJob */
/** @typedef {{id:number,name:string,job_id:number,destination:string,next_run:number,enabled:boolean}} ReportSchedule */
/** @typedef {{id:number,report_id:number,scheduled_for:number,status:'delivered'|'failed'|'processing'|'pending'|'skipped',attempts:number,created_at:number,finished_at:number|null,checksum:string,filename:string,destination:string,error:string}} Delivery */
export {};

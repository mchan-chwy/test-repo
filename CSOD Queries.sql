-- Transcript Information
select 
  -- trn."lo_status_types",
  trn_l.title as learning_object_title,
  trn_l.descr as learning_object_description,
  trn_l.culture_description as learning_object_culture,
  trn_l.language_title as learning_object_language,
  -- trn."lo_object_types",
  tt.description as object_type_description,
  -- trn."training_version_effective_dts",
  -- trn."training_version_start_dts",
  -- trn."training_version_end_dts",
  -- trs."user_lo_status_ids",
  trs_usr.user_ref as employee_id_csod,
  -- trs_usr.user_name_first as employee_first_name,
  -- trs_usr.user_name_last as employee_last_name,
  concat(trs_usr.user_name_first,' ',trs_usr.user_name_last) as employee_full_name,
  trs_usr.user_login as employee_username_csod,
  trs.user_lo_score as user_score,
  cast(trs.user_lo_create_dt as date) as transcript_item_creation_date,
  cast(trs.user_lo_reg_dt as date) as transcript_item_registration_date,
  cast(trs.user_lo_start_dt as date) as transcript_item_start_date,
  cast(trs.user_lo_comp_dt as date) as transcript_item_completion_date,
  cast(trs.user_lo_assigned_dt as date) as transcript_item_assigned_date,
  cast(trs.user_lo_last_access_dt as date) as transcript_item_last_accessed_date,
  cast(trs.user_lo_last_action_dt as datetime) as transcript_item_last_action_date,
  cast(trs.user_lo_last_modified_dt as date) as transcript_item_last_modified_date,
  cast(trs._last_touched_dt_utc as datetime) as transcript_last_touched_date_utc,
  trs.user_lo_minutes_participated as user_participation_in_minutes,
  --"user_lo_num_attempts",
  trs.user_lo_pct_complete as completion_percentage,
  trs.is_assigned as assigned_flag,
  trs.is_required as required_flag,
  trs.is_suggested as suggested_flag,
  -- trs."transc_user_ids",
  --"user_lo_status_group_ids",
  trs.is_latest_version_on_transcript as latest_version_on_transcript_flag,
  rst.*
from d_hrdatamart.s_cornerstone.transcript_core trs
left join d_hrdatamart.s_cornerstone.users_core trs_usr
  on trs.transc_user_id = trs_usr.user_id
left join d_hrdatamart.s_cornerstone.training_core trn
  on trs.transc_object_id = trn.object_id
left join d_hrdatamart.s_cornerstone.training_type_core tt
  on trn.lo_object_type = tt.object_type
left join (select clt.culture_name, clt.descr as culture_description, lng.title as language_title ,trn_l.* 
           from d_hrdatamart.s_cornerstone.training_local_core trn_l 
           left join d_hrdatamart.s_cornerstone.culture_core clt 
             on trn_l.culture_id = clt.culture_id
           left join d_hrdatamart.s_cornerstone.language_core lng
            on trn_l.culture_id = lng.culture_id
           where trn_l.culture_id = 1) trn_l
  on trn.object_id = trn_l.object_id
left join d_hrdatamart.s_reference.common_date dt
  on cast(trs.user_lo_assigned_dt as date) = dt.common_date_dttm
left join d_hrdatamart.s_analytics.roster_week_end rst
  on right(concat('000000',cast(rst.employee_id as string)),6) = trs_usr.user_ref
  and dt.financial_calendar_reporting_week = rst.reporting_year_week;





-- Working on this one to get the stuff done for CS TSAT (Training Satisfaction)
select 
    concat(evl."user_name_firsts", ' ', evl."user_name_lasts") as evaluator_full_name, 
    concat(usr."user_name_firsts", ' ', usr."user_name_lasts") as user_full_name,
    tl.title as learning_object_title,
    tl.descr as learning_object_description,
    trn."lo_location_ids",
    ou_info.ou_title,
    -- ass."qna_container_ids",
    -- ass."reg_nums",
    -- ass."session_ids",
    ass."submitted_dts" as assessment_submission_date,
    cnt_txt."titles" as container_title,
    qs_txt."titles" as question_title,
    rst."comments" as assessment_result_comment,
    rsp."response_texts" as assessment_response_texts,
    concat(ans_by."user_name_firsts", ' ', ans_by."user_name_lasts") as answer_by_full_name,
    ans_txt."titles" as answer_title,
    rsp.*,
    -- rst.*,
    str.*,
    ass.* 
from d_hrdatamart_sbx.s_analytics_sbx.csod_assessment_evaluation_core ass
left join d_hrdatamart_sbx.s_analytics_sbx.csod_users_core evl
  on ass."evaluator_user_ids" = evl."user_ids"
left join d_hrdatamart_sbx.s_analytics_sbx.csod_users_core usr
  on ass."user_ids" = usr."user_ids"
left join d_hrdatamart_sbx.s_analytics_sbx.csod_training_core trn
  on ass."object_ids" = trn."object_ids"
left join (select clt.culture_name, clt.descr as culture_description, lng.title as language_title ,trn_l.* 
           from d_hrdatamart.s_cornerstone.training_local_core trn_l
           left join d_hrdatamart.s_cornerstone.culture_core clt 
             on trn_l.culture_id = clt.culture_id
           left join d_hrdatamart.s_cornerstone.language_core lng
            on trn_l.culture_id = lng.culture_id
           where trn_l.culture_id = 1) tl
  on trn."object_ids" = tl.object_id
left join d_hrdatamart_sbx.s_analytics_sbx.csod_qna_container_core cnt
  on ass."qna_container_ids" = cnt."container_ids"
left join d_hrdatamart_sbx.s_analytics_sbx.csod_qna_text_local_core cnt_txt
  on cnt."qna_text_ids" = cnt_txt."qna_text_ids"
  and cnt_txt."culture_ids" = 1
left join d_hrdatamart_sbx.s_analytics_sbx.csod_qna_structure_core str
  on cnt."container_ids" = str."container_ids"
left join d_hrdatamart_sbx.s_analytics_sbx.csod_qna_question_core qs
  on str."question_ids" = qs."question_ids"
left join d_hrdatamart_sbx.s_analytics_sbx.csod_qna_text_local_core qs_txt
  on qs."qna_text_ids" = qs_txt."qna_text_ids"
  and qs_txt."culture_ids" = 1
-- where evl."user_ids" = 36674
left join d_hrdatamart_sbx.s_analytics_sbx.csod_assessment_result_core rst
  on rst."session_ids" = ass."session_ids"
  and rst."question_instance_ids" = str."question_instance_ids"
left join d_hrdatamart_sbx.s_analytics_sbx.csod_assessment_response_core rsp
  on rst."assessment_result_ids" = rsp."assessment_result_ids"
left join d_hrdatamart_sbx.s_analytics_sbx.csod_qna_answer_bank_core ans
  on rsp."answer_item_ids" = ans."answer_item_ids"
left join d_hrdatamart_sbx.s_analytics_sbx.csod_users_core ans_by
  on ans."created_by_user_ids" = ans_by."user_ids"
left join d_hrdatamart_sbx.s_analytics_sbx.csod_qna_text_local_core ans_txt
  on ans."qna_text_ids" = ans_txt."qna_text_ids"
  and ans_txt."culture_ids" = 1
left join (select 
              ou_typ."type_names" as ou_type,
              ou."titles" as ou_title,
              prnt_typ."type_names" as parent_ou_type,
              prnt_ou."titles" as parent_ou_title,
              ou."ou_ids"
            from d_hrdatamart_sbx.s_analytics_sbx.csod_ou_core ou
            left join d_hrdatamart_sbx.s_analytics_sbx.csod_ou_type_core ou_typ
              on ou."type_ids" = ou_typ."ou_type_ids"
            left join d_hrdatamart_sbx.s_analytics_sbx.csod_ou_core prnt_ou
              on ou."parent_ids" = prnt_ou."ou_ids"
            left join d_hrdatamart_sbx.s_analytics_sbx.csod_ou_type_core prnt_typ
              on prnt_ou."type_ids" = prnt_typ."ou_type_ids") ou_info
  on ou_info."ou_ids" = trn."lo_location_ids"
  and ou_info.ou_type = 'Location'
where cnt_txt."titles" = 'V/ILT Standard Evaluation'
  and ass."submitted_dts" is not null
  and tl.title like '%Chewy Leader Onboarding%'
;




-- Questions
select 
    tx."titles" as question_title, 
    tx."descrs" as question_description, 
    typ."titles" as question_type_title, 
    cat."titles" as question_category_title, 
    concat(cb."user_name_firsts",' ', cb."user_name_lasts") as created_by_full_name,
    -- cat."assessment_type_masks", 
    -- ass."titles",
    q.*
from d_hrdatamart_sbx.s_analytics_sbx.csod_qna_question_core q
left join d_hrdatamart_sbx.s_analytics_sbx.csod_qna_text_local_core tx
  on q."qna_text_ids" = tx."qna_text_ids"
  and tx."culture_ids" = 1
left join d_hrdatamart_sbx.s_analytics_sbx.csod_qna_question_type_local_core typ
  on q."qna_type_ids" = typ."qna_type_ids"
  and typ."culture_ids" = 1
left join d_hrdatamart_sbx.s_analytics_sbx.csod_qna_question_category_core cat
  on q."qna_category_ids" = cat."qna_category_ids"
left join d_hrdatamart_sbx.s_analytics_sbx.csod_assessment_type_core ass_typ
  on cat."assessment_type_masks" = ass_typ."assessment_type_flags"
left join d_hrdatamart_sbx.s_analytics_sbx.csod_users_core cb
  on q."created_by_user_ids" = cb."user_ids"




-- Organizational Units
select 
  ou_typ."type_names" as ou_type,
  ou."titles" as ou_title,
  prnt_typ."type_names" as parent_ou_type,
  prnt_ou."titles" as parent_ou_title,
  ou.*
from d_hrdatamart_sbx.s_analytics_sbx.csod_ou_core ou
left join d_hrdatamart_sbx.s_analytics_sbx.csod_ou_type_core ou_typ
  on ou."type_ids" = ou_typ."ou_type_ids"
left join d_hrdatamart_sbx.s_analytics_sbx.csod_ou_core prnt_ou
  on ou."parent_ids" = prnt_ou."ou_ids"
left join d_hrdatamart_sbx.s_analytics_sbx.csod_ou_type_core prnt_typ
  on prnt_ou."type_ids" = prnt_typ."ou_type_ids"
;


--User Hierarchy
 select usr_ou."user_ids", ou."titles"
  from d_hrdatamart_sbx.s_analytics_sbx.csod_user_ou_core usr_ou
  -- on usr."user_ids" = usr_ou."user_ids"
  left join d_hrdatamart_sbx.s_analytics_sbx.csod_ou_type_core ou_typ
    on usr_ou."ou_type_ids" = ou_typ."ou_type_ids"
  left join d_hrdatamart_sbx.s_analytics_sbx.csod_ou_core ou
    on usr_ou."ou_ids" = ou."ou_ids"


-- User
select 
    -- ou_typ."type_names",
    ou_loc."titles" as location,
    ou_cch."titles" as chewy_cost_center_hierarchy,
    ou_so."titles" as supervisory_org,
    ou_ml."titles" as management_level,
    ou_jp."titles" as job_profile_name,
    usr."user_name_firsts",
    usr."user_name_lasts",
    usr."user_ids",
    usr."user_emails",
    usr."user_hire_dt_lasts",
    usr."user_hire_dt_origs",
    usr."user_last_logins",
    usr."user_logins",
    usr."user_refs",
    -- usr.*,
    1
from d_hrdatamart_sbx.s_analytics_sbx.csod_users_core usr
left join (
    select usr_ou."user_ids", ou."titles"
    from d_hrdatamart_sbx.s_analytics_sbx.csod_user_ou_core usr_ou
    -- on usr."user_ids" = usr_ou."user_ids"
    left join d_hrdatamart_sbx.s_analytics_sbx.csod_ou_type_core ou_typ
    on usr_ou."ou_type_ids" = ou_typ."ou_type_ids"
    left join d_hrdatamart_sbx.s_analytics_sbx.csod_ou_core ou
    on usr_ou."ou_ids" = ou."ou_ids"
    where ou_typ."type_names" = 'Location'
) ou_loc
    on usr."user_ids" = ou_loc."user_ids"
left join (
    select usr_ou."user_ids", ou."titles"
    from d_hrdatamart_sbx.s_analytics_sbx.csod_user_ou_core usr_ou
    -- on usr."user_ids" = usr_ou."user_ids"
    left join d_hrdatamart_sbx.s_analytics_sbx.csod_ou_type_core ou_typ
    on usr_ou."ou_type_ids" = ou_typ."ou_type_ids"
    left join d_hrdatamart_sbx.s_analytics_sbx.csod_ou_core ou
    on usr_ou."ou_ids" = ou."ou_ids"
    where ou_typ."type_names" = 'Chewy Cost Center Hierarchy'
) ou_cch
    on usr."user_ids" = ou_cch."user_ids"
left join (
    select usr_ou."user_ids", ou."titles"
    from d_hrdatamart_sbx.s_analytics_sbx.csod_user_ou_core usr_ou
    -- on usr."user_ids" = usr_ou."user_ids"
    left join d_hrdatamart_sbx.s_analytics_sbx.csod_ou_type_core ou_typ
    on usr_ou."ou_type_ids" = ou_typ."ou_type_ids"
    left join d_hrdatamart_sbx.s_analytics_sbx.csod_ou_core ou
    on usr_ou."ou_ids" = ou."ou_ids"
    where ou_typ."type_names" = 'Supervisory Org'
) ou_so
    on usr."user_ids" = ou_so."user_ids"
left join (
    select usr_ou."user_ids", ou."titles"
    from d_hrdatamart_sbx.s_analytics_sbx.csod_user_ou_core usr_ou
    -- on usr."user_ids" = usr_ou."user_ids"
    left join d_hrdatamart_sbx.s_analytics_sbx.csod_ou_type_core ou_typ
    on usr_ou."ou_type_ids" = ou_typ."ou_type_ids"
    left join d_hrdatamart_sbx.s_analytics_sbx.csod_ou_core ou
    on usr_ou."ou_ids" = ou."ou_ids"
    where ou_typ."type_names" = 'Management Level'
) ou_ml
    on usr."user_ids" = ou_ml."user_ids"
left join (
    select usr_ou."user_ids", ou."titles"
    from d_hrdatamart_sbx.s_analytics_sbx.csod_user_ou_core usr_ou
    -- on usr."user_ids" = usr_ou."user_ids"
    left join d_hrdatamart_sbx.s_analytics_sbx.csod_ou_type_core ou_typ
    on usr_ou."ou_type_ids" = ou_typ."ou_type_ids"
    left join d_hrdatamart_sbx.s_analytics_sbx.csod_ou_core ou
    on usr_ou."ou_ids" = ou."ou_ids"
    where ou_typ."type_names" = 'Job Profile Name'
) ou_jp
    on usr."user_ids" = ou_jp."user_ids"
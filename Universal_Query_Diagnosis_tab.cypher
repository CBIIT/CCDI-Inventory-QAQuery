Match (st:study)
where st.dbgap_accession in ['']
with st
Call {
  with st
  MATCH (st)<--(p:participant)<--(dg:diagnosis)
  optional MATCH (p)<-[:of_sample]-(sm1:sample)<--(cl)<--(sm2:sample)
  WHERE (cl: cell_line or cl: pdx)
  optional Match (sm2)<--(file)
  WHERE (file: sequencing_file OR file:pathology_file OR file:methylation_array_file OR file:cytogenomic_file) 
  with p, case COLLECT(distinct sm1) when [] then []
                else COLLECT(DISTINCT {
                        sample_anatomic_site: sm1.anatomic_site,
                        participant_age_at_collection: sm1.participant_age_at_collection,
                        sample_tumor_status: sm1.sample_tumor_status,
                        tumor_classification: sm1.tumor_classification,
                        assay_method: CASE LABELS(file)[0]
                                  WHEN 'sequencing_file' THEN 'Sequencing'
                                  WHEN 'cytogenomic_file' THEN 'Cytogenomic'
                                  WHEN 'pathology_file' THEN 'Pathology imaging'
                                  WHEN 'methylation_array_file' THEN 'Methylation array'
                                  ELSE null END,
                        file_type: CASE LABELS(file)[0]
                                  When null then null
                                  else file.file_type end,
                        library_selection: CASE LABELS(file)[0]
                                      WHEN 'sequencing_file' THEN file.library_selection
                                      ELSE null END,
                        library_source_material: CASE LABELS(file)[0]
                                    WHEN 'sequencing_file' THEN file.library_source_material
                                    ELSE null END,
                        library_source_molecule: CASE LABELS(file)[0]
                                    WHEN 'sequencing_file' THEN file.library_source_molecule
                                    ELSE null END,
                        library_strategy: CASE LABELS(file)[0]
                                      WHEN 'sequencing_file' THEN file.library_strategy
                                      ELSE null END
                    }) end AS sample1,
                    case COLLECT(distinct sm2) 
                    when [] then []
                    else COLLECT(DISTINCT {
                        sample_anatomic_site: sm2.anatomic_site,
                        participant_age_at_collection: sm2.participant_age_at_collection,
                        sample_tumor_status: sm2.sample_tumor_status,
                        tumor_classification: sm2.tumor_classification,
                        assay_method: CASE LABELS(file)[0]
                                  WHEN 'sequencing_file' THEN 'Sequencing'
                                  WHEN 'cytogenomic_file' THEN 'Cytogenomic'
                                  WHEN 'pathology_file' THEN 'Pathology imaging'
                                  WHEN 'methylation_array_file' THEN 'Methylation array'
                                  ELSE null END,
                        file_type: CASE LABELS(file)[0]
                                  When null then null
                                  else file.file_type end,
                        library_selection: CASE LABELS(file)[0]
                                      WHEN 'sequencing_file' THEN file.library_selection
                                      ELSE null END,
                        library_source_material: CASE LABELS(file)[0]
                                    WHEN 'sequencing_file' THEN file.library_source_material
                                    ELSE null END,
                        library_source_molecule: CASE LABELS(file)[0]
                                    WHEN 'sequencing_file' THEN file.library_source_molecule
                                    ELSE null END,
                        library_strategy: CASE LABELS(file)[0]
                                      WHEN 'sequencing_file' THEN file.library_strategy
                                      ELSE null END
                    }) end AS sample2
  with p, apoc.coll.union(sample1,sample2) as cell_line_pdx_file_filters
  OPTIONAL MATCH (p)<-[:of_sample]-(sm:sample)<--(file)
  WHERE (file: sequencing_file OR file:pathology_file OR file:methylation_array_file OR file:cytogenomic_file)
  with p, cell_line_pdx_file_filters, COLLECT(DISTINCT {
                sample_anatomic_site: sm.anatomic_site,
                participant_age_at_collection: sm.participant_age_at_collection,
                sample_tumor_status: sm.sample_tumor_status,
                tumor_classification: sm.tumor_classification,
                assay_method: CASE LABELS(file)[0]
                          WHEN 'sequencing_file' THEN 'Sequencing'
                          WHEN 'cytogenomic_file' THEN 'Cytogenomic'
                          WHEN 'pathology_file' THEN 'Pathology imaging'
                          WHEN 'methylation_array_file' THEN 'Methylation array' 
                          ELSE null END,
                file_type: file.file_type,
                library_selection: CASE LABELS(file)[0]
                              WHEN 'sequencing_file' THEN file.library_selection
                              ELSE null END,
                library_source_material: CASE LABELS(file)[0]
                                    WHEN 'sequencing_file' THEN file.library_source_material
                                    ELSE null END,
                library_source_molecule: CASE LABELS(file)[0]
                                    WHEN 'sequencing_file' THEN file.library_source_molecule
                                    ELSE null END,
                library_strategy: CASE LABELS(file)[0]
                              WHEN 'sequencing_file' THEN file.library_strategy
                              ELSE null END
            }) AS general_file_filters
  OPTIONAL Match (p)<-[:of_sample]-(sm:sample)
  OPTIONAL MATCH (p)<-[:of_clinical_measure_file]-(file1:clinical_measure_file)
  with p, cell_line_pdx_file_filters, general_file_filters,sm, COLLECT(DISTINCT file1.file_type) as file1_types
  UNWIND (case file1_types when [] then [null] else file1_types end)  AS types_1
  with p, cell_line_pdx_file_filters, general_file_filters, COLLECT(DISTINCT {
            sample_anatomic_site: sm.anatomic_site,
            participant_age_at_collection: sm.participant_age_at_collection,
            sample_tumor_status: sm.sample_tumor_status,
            tumor_classification: sm.tumor_classification,
            assay_method: CASE types_1 when null then null else 'Clinical data' end,
            file_type: types_1,
            library_selection: null,
            library_source_material: null,
            library_source_molecule: null,
            library_strategy: null
    }) as participant_clinical_measure_file_filters
  OPTIONAL Match (p)<-[:of_sample]-(sm:sample)
  OPTIONAL MATCH (p)<-[:of_radiology_file]-(file1:radiology_file)
  with p, cell_line_pdx_file_filters, general_file_filters, participant_clinical_measure_file_filters, sm, COLLECT(DISTINCT file1.file_type) as file1_types
  UNWIND (case file1_types when [] then [null] else file1_types end)  AS types_1
  with p, cell_line_pdx_file_filters, general_file_filters, participant_clinical_measure_file_filters, COLLECT(DISTINCT {
            sample_anatomic_site: sm.anatomic_site,
            participant_age_at_collection: sm.participant_age_at_collection,
            sample_tumor_status: sm.sample_tumor_status,
            tumor_classification: sm.tumor_classification,
            assay_method: CASE types_1 when null then null else 'Radiology imaging' end,
            file_type: types_1,
            library_selection: null,
            library_source_material: null,
            library_source_molecule: null,
            library_strategy: null
    }) as participant_radiology_file_filters
  MATCH (dg:diagnosis)
  MATCH (p)<-[:of_diagnosis]-(dg)
  OPTIONAL MATCH (p)<-[*..4]-(file)
  WHERE (file:clinical_measure_file OR file: sequencing_file OR file:pathology_file OR file:radiology_file OR file:methylation_array_file OR file:cytogenomic_file)
  OPTIONAL MATCH (p)<-[:of_survival]-(su:survival)
  with p, cell_line_pdx_file_filters, general_file_filters, participant_clinical_measure_file_filters,participant_radiology_file_filters, dg, file, su
  OPTIONAL MATCH (st:study)<-[:of_participant]-(p)
  OPTIONAL MATCH (st)<-[:of_study_personnel]-(stp:study_personnel)
  OPTIONAL MATCH (st)<-[:of_study_funding]-(stf:study_funding)
  WITH p, cell_line_pdx_file_filters, general_file_filters, participant_clinical_measure_file_filters,participant_radiology_file_filters, file, COLLECT(DISTINCT su.last_known_survival_status) as vital_status, st, stf, stp, dg
  RETURN DISTINCT
    dg.id as id,
    p.id as pid,
    null as sid,
    null as sample_id,
    dg.diagnosis_id as diagnosis_id,
    dg.diagnosis as diagnosis,
    dg.disease_phase as disease_phase,
    dg.diagnosis_classification_system as diagnosis_classification_system,
    dg.diagnosis_basis as diagnosis_basis,
    dg.tumor_grade_source as tumor_grade_source,
    dg.tumor_stage_source as tumor_stage_source,
    dg.anatomic_site as diagnosis_anatomic_site,
    dg.age_at_diagnosis as age_at_diagnosis,
    p.participant_id as participant_id,
    apoc.text.split(p.race, ';') as race,
    p.sex_at_birth as sex_at_birth,
    st.study_id as study_id,
    st.dbgap_accession as dbgap_accession,
    st.study_acronym as study_acronym,
    st.study_name as study_name,
    case when 'Dead' in vital_status then ['Dead']
          else vital_status end as last_known_survival_status,       
    apoc.coll.union(cell_line_pdx_file_filters, general_file_filters) + participant_clinical_measure_file_filters + participant_radiology_file_filters AS sample_file_filters
  union all
  with st
  match (st)<--(p:participant)<--(sm:sample)<--(dg:diagnosis)
  optional match (sm)<-[*..3]-(file)
  where (file: sequencing_file OR file:pathology_file OR file:methylation_array_file OR file:cytogenomic_file)
  with dg, COLLECT(DISTINCT {
            sample_anatomic_site: sm.anatomic_site,
            participant_age_at_collection: sm.participant_age_at_collection,
            sample_tumor_status: sm.sample_tumor_status,
            tumor_classification: sm.tumor_classification,
            assay_method: CASE LABELS(file)[0]
                          WHEN 'sequencing_file' THEN 'Sequencing'
                          WHEN 'cytogenomic_file' THEN 'Cytogenomic'
                          WHEN 'pathology_file' THEN 'Pathology imaging'
                          WHEN 'methylation_array_file' THEN 'Methylation array'
                          ELSE null END,
            file_type: file.file_type,
            library_selection: CASE LABELS(file)[0]
                      WHEN 'sequencing_file' THEN file.library_selection
                      ELSE null END,
            library_source_material: CASE LABELS(file)[0]
                      WHEN 'sequencing_file' THEN file.library_source_material
                      ELSE null END,
            library_source_molecule: CASE LABELS(file)[0]
                      WHEN 'sequencing_file' THEN file.library_source_molecule
                      ELSE null END,
            library_strategy: CASE LABELS(file)[0]
                      WHEN 'sequencing_file' THEN file.library_strategy
                      ELSE null END
    }) as sample_file_filter_1
  match (p:participant)<--(sm1:sample)<--(dg)
  optional match (sm1)<--(cl)<--(sm:sample)
  where (cl: cell_line or cl: pdx)
  optional match (sm)<--(file)
  where (file: sequencing_file OR file:pathology_file OR file:methylation_array_file OR file:cytogenomic_file)
  with dg, sample_file_filter_1, COLLECT(DISTINCT {
            sample_anatomic_site: sm.anatomic_site,
            participant_age_at_collection: sm.participant_age_at_collection,
            sample_tumor_status: sm.sample_tumor_status,
            tumor_classification: sm.tumor_classification,
            assay_method: CASE LABELS(file)[0]
                          WHEN 'sequencing_file' THEN 'Sequencing'
                          WHEN 'cytogenomic_file' THEN 'Cytogenomic'
                          WHEN 'pathology_file' THEN 'Pathology imaging'
                          WHEN 'methylation_array_file' THEN 'Methylation array'
                          ELSE null END,
            file_type: file.file_type,
            library_selection: CASE LABELS(file)[0]
                      WHEN 'sequencing_file' THEN file.library_selection
                      ELSE null END,
            library_source_material: CASE LABELS(file)[0]
                      WHEN 'sequencing_file' THEN file.library_source_material
                      ELSE null END,
            library_source_molecule: CASE LABELS(file)[0]
                      WHEN 'sequencing_file' THEN file.library_source_molecule
                      ELSE null END,
            library_strategy: CASE LABELS(file)[0]
                      WHEN 'sequencing_file' THEN file.library_strategy
                      ELSE null END
    }) as sample_file_filter_2
  with dg, apoc.coll.union(sample_file_filter_1, sample_file_filter_2) as sample_file_filter
  match (p:participant)<--(sm:sample)<--(dg)
  optional match (sm)<--(file)
  OPTIONAL MATCH (p)<-[:of_survival]-(su:survival)
  OPTIONAL MATCH (st:study)<-[:of_participant]-(p)
  OPTIONAL MATCH (st)<-[:of_study_personnel]-(stp:study_personnel)
  OPTIONAL MATCH (st)<-[:of_study_funding]-(stf:study_funding)
  WITH dg, p, sm, sample_file_filter, file, COLLECT(DISTINCT su.last_known_survival_status) as vital_status, st, stf, stp
  RETURN DISTINCT
    dg.id as id,
    p.id as pid,
    sm.id as sid,
    sm.sample_id as sample_id,
    dg.diagnosis_id as diagnosis_id,
    dg.diagnosis as diagnosis,
    dg.disease_phase as disease_phase,
    dg.diagnosis_classification_system as diagnosis_classification_system,
    dg.diagnosis_basis as diagnosis_basis,
    dg.tumor_grade_source as tumor_grade_source,
    dg.tumor_stage_source as tumor_stage_source,
    dg.anatomic_site as diagnosis_anatomic_site,
    dg.age_at_diagnosis as age_at_diagnosis,
    p.participant_id as participant_id,
    apoc.text.split(p.race, ';') as race,
    p.sex_at_birth as sex_at_birth,
    st.study_id as study_id,
    st.dbgap_accession as dbgap_accession,
    st.study_acronym as study_acronym,
    st.study_name as study_name,
    case when 'Dead' in vital_status then ['Dead']
          else vital_status end as last_known_survival_status,       
    sample_file_filter AS sample_file_filters
  union all
  with st
  MATCH (st)<--(p:participant)<-[:of_sample]-(sm1:sample)<--(cl)<--(sm2:sample)<--(dg:diagnosis)
  WHERE (cl: cell_line or cl: pdx)
  optional Match (sm1)<--(file1)
  WHERE (file1: sequencing_file OR file1:pathology_file OR file1:methylation_array_file OR file1:cytogenomic_file)
  optional Match (sm2)<--(file2)
  WHERE (file2: sequencing_file OR file2:pathology_file OR file2:methylation_array_file OR file2:cytogenomic_file)
  with dg, collect(distinct sm1) as sm1_list, collect(distinct sm2) as sm2_list, collect(distinct file1) as file1_list, collect(distinct file2) as file2_list
  with dg, apoc.coll.union(sm1_list, sm2_list) as samples, apoc.coll.union(file1_list, file2_list) as files
  unwind samples as sample
  with dg, sample, files
  UNWIND (case files when [] then [null] else files end)  AS file
  with dg, sample, file
  with dg, COLLECT(DISTINCT {
            sample_anatomic_site: sample.anatomic_site,
            participant_age_at_collection: sample.participant_age_at_collection,
            sample_tumor_status: sample.sample_tumor_status,
            tumor_classification: sample.tumor_classification,
            assay_method: CASE LABELS(file)[0]
                          WHEN 'sequencing_file' THEN 'Sequencing'
                          WHEN 'cytogenomic_file' THEN 'Cytogenomic'
                          WHEN 'pathology_file' THEN 'Pathology imaging'
                          WHEN 'methylation_array_file' THEN 'Methylation array'
                          ELSE null END,
            file_type: file.file_type,
            library_selection: CASE LABELS(file)[0]
                      WHEN 'sequencing_file' THEN file.library_selection
                      ELSE null END,
            library_source_material: CASE LABELS(file)[0]
                      WHEN 'sequencing_file' THEN file.library_source_material
                      ELSE null END,
            library_source_molecule: CASE LABELS(file)[0]
                      WHEN 'sequencing_file' THEN file.library_source_molecule
                      ELSE null END,
            library_strategy: CASE LABELS(file)[0]
                      WHEN 'sequencing_file' THEN file.library_strategy
                      ELSE null END
    }) as sample_file_filter
  optional match (p:participant)<-[:of_sample]-(sm1:sample)<--(cl)<--(sm:sample)<--(dg)
  WHERE (cl: cell_line or cl: pdx)
  optional match (sm)<--(file)
  WHERE (file: sequencing_file OR file:pathology_file OR file:methylation_array_file OR file:cytogenomic_file)
  with dg, sample_file_filter, collect(distinct file.id) as files, apoc.coll.union(collect(distinct sm1.id), collect(distinct sm.id)) as sid, apoc.coll.union(collect(distinct sm1.sample_id), collect(distinct sm.sample_id))  as sample_id
  optional match (p:participant)<-[*..4]-(dg)
  OPTIONAL MATCH (p)<-[:of_survival]-(su:survival)
  OPTIONAL MATCH (st:study)<-[:of_participant]-(p)
  OPTIONAL MATCH (st)<-[:of_study_personnel]-(stp:study_personnel)
  OPTIONAL MATCH (st)<-[:of_study_funding]-(stf:study_funding)
  WITH dg, p, sid, sample_id, sample_file_filter, files, COLLECT(DISTINCT su.last_known_survival_status) as vital_status, st, stf, stp
  RETURN DISTINCT
    dg.id as id,
    p.id as pid,
    sid as sid,
    sample_id as sample_id,
    dg.diagnosis_id as diagnosis_id,
    dg.diagnosis as diagnosis,
    dg.disease_phase as disease_phase,
    dg.diagnosis_classification_system as diagnosis_classification_system,
    dg.diagnosis_basis as diagnosis_basis,
    dg.tumor_grade_source as tumor_grade_source,
    dg.tumor_stage_source as tumor_stage_source,
    dg.anatomic_site as diagnosis_anatomic_site,
    dg.age_at_diagnosis as age_at_diagnosis,
    p.participant_id as participant_id,
    apoc.text.split(p.race, ';') as race,
    p.sex_at_birth as sex_at_birth,
    st.study_id as study_id,
    st.dbgap_accession as dbgap_accession,
    st.study_acronym as study_acronym,
    st.study_name as study_name,
    case when 'Dead' in vital_status then ['Dead']
          else vital_status end as last_known_survival_status,       
    sample_file_filter AS sample_file_filters
  union all
  with st
  match (st)<--(cl)<--(sm:sample)<--(dg:diagnosis)
  where (cl: cell_line or cl: pdx)
  optional match (sm)<--(file)
  where (file: sequencing_file OR file:pathology_file OR file:methylation_array_file OR file:cytogenomic_file)
  with dg, COLLECT(DISTINCT {
            sample_anatomic_site: sm.anatomic_site,
            participant_age_at_collection: sm.participant_age_at_collection,
            sample_tumor_status: sm.sample_tumor_status,
            tumor_classification: sm.tumor_classification,
            assay_method: CASE LABELS(file)[0]
                          WHEN 'sequencing_file' THEN 'Sequencing'
                          WHEN 'cytogenomic_file' THEN 'Cytogenomic'
                          WHEN 'pathology_file' THEN 'Pathology imaging'
                          WHEN 'methylation_array_file' THEN 'Methylation array'
                          ELSE null END,
            file_type: file.file_type,
            library_selection: CASE LABELS(file)[0]
                      WHEN 'sequencing_file' THEN file.library_selection
                      ELSE null END,
            library_source_material: CASE LABELS(file)[0]
                      WHEN 'sequencing_file' THEN file.library_source_material
                      ELSE null END,
            library_source_molecule: CASE LABELS(file)[0]
                      WHEN 'sequencing_file' THEN file.library_source_molecule
                      ELSE null END,
            library_strategy: CASE LABELS(file)[0]
                      WHEN 'sequencing_file' THEN file.library_strategy
                      ELSE null END
    }) as sample_file_filter
  optional match (st:study)<--(cl)<--(sm:sample)<--(dg)
  where (cl: cell_line or cl: pdx)
  optional match (sm)<--(file)
  where (file: sequencing_file OR file:pathology_file OR file:methylation_array_file OR file:cytogenomic_file)
  OPTIONAL MATCH (st)<-[:of_study_personnel]-(stp:study_personnel)
  OPTIONAL MATCH (st)<-[:of_study_funding]-(stf:study_funding)
  WITH dg, sm, sample_file_filter, file, st, stf, stp
  RETURN DISTINCT
    dg.id as id,
    null as pid,
    sm.id as sid,
    sm.sample_id as sample_id,
    dg.diagnosis_id as diagnosis_id,
    dg.diagnosis as diagnosis,
    dg.disease_phase as disease_phase,
    dg.diagnosis_classification_system as diagnosis_classification_system,
    dg.diagnosis_basis as diagnosis_basis,
    dg.tumor_grade_source as tumor_grade_source,
    dg.tumor_stage_source as tumor_stage_source,
    dg.anatomic_site as diagnosis_anatomic_site,
    dg.age_at_diagnosis as age_at_diagnosis,
    null as participant_id,
    null as race,
    null as sex_at_birth,
    st.study_id as study_id,
    st.dbgap_accession as dbgap_accession,
    st.study_acronym as study_acronym,
    st.study_name as study_name,
    null as last_known_survival_status,       
    sample_file_filter AS sample_file_filters
}
with id, participant_id, sample_id, diagnosis_id, dbgap_accession, study_acronym, study_name, sex_at_birth, race, age_at_diagnosis, diagnosis, diagnosis_anatomic_site, diagnosis_classification_system, diagnosis_basis, disease_phase, last_known_survival_status, sample_file_filters
where study_acronym in [''] and study_name in ['']
with id, participant_id, sample_id, diagnosis_id, dbgap_accession, sex_at_birth, race, age_at_diagnosis, diagnosis, diagnosis_anatomic_site, diagnosis_classification_system, diagnosis_basis, disease_phase, last_known_survival_status, sample_file_filters
where participant_id in [''] and sex_at_birth in [''] and ANY(element IN [''] WHERE element IN race)
with id, participant_id, sample_id, diagnosis_id, dbgap_accession, age_at_diagnosis, diagnosis, diagnosis_anatomic_site, diagnosis_classification_system, diagnosis_basis, disease_phase, last_known_survival_status, sample_file_filters
where age_at_diagnosis >= [''] and age_at_diagnosis <= [''] and diagnosis in [''] and diagnosis_anatomic_site in [''] and diagnosis_classification_system in [''] and diagnosis_basis in [''] and disease_phase in ['']
with id, participant_id, sample_id, diagnosis_id, dbgap_accession, age_at_diagnosis, diagnosis, diagnosis_anatomic_site, diagnosis_classification_system, diagnosis_basis, disease_phase, last_known_survival_status, sample_file_filters
where ANY(element IN [''] WHERE element IN last_known_survival_status) 
unwind sample_file_filters as sample_file_filter
with id, participant_id, sample_id, diagnosis_id, dbgap_accession, age_at_diagnosis, diagnosis, diagnosis_anatomic_site, diagnosis_classification_system, diagnosis_basis, disease_phase, last_known_survival_status, sample_file_filter
where sample_file_filter.participant_age_at_collection >= [''] and sample_file_filter.participant_age_at_collection <= [''] and sample_file_filter.sample_anatomic_site in [''] and sample_file_filter.sample_tumor_status in [''] and sample_file_filter.tumor_classification in [''] 
      and sample_file_filter.assay_method in [''] and sample_file_filter.file_type in [''] 
      and sample_file_filter.library_selection in [''] and sample_file_filter.library_source_material in [''] and sample_file_filter.library_source_molecule in [''] and sample_file_filter.library_strategy in ['']
with id, participant_id, sample_id, dbgap_accession, age_at_diagnosis, diagnosis, diagnosis_anatomic_site, diagnosis_classification_system, diagnosis_basis, disease_phase, last_known_survival_status
with distinct id, participant_id, sample_id, dbgap_accession, age_at_diagnosis, diagnosis, diagnosis_anatomic_site, diagnosis_classification_system, diagnosis_basis, disease_phase, last_known_survival_status
return
coalesce(participant_id, '') as `Participant ID`,
coalesce(sample_id, '') as `Sample ID`,
coalesce(dbgap_accession, '') as `Study ID`,
coalesce(diagnosis, '') as `Diagnosis`,
coalesce(diagnosis_anatomic_site, '') as `Diagnosis Anatomic Site`,
coalesce(diagnosis_classification_system, '') as `Diagnosis Classification System`,
coalesce(diagnosis_basis, '') as `Diagnosis Basis`,
coalesce(disease_phase, '') as `Disease Phase`,
case age_at_diagnosis when -999 then 'Not Reported' else coalesce(age_at_diagnosis, '') end as `Age at diagnosis (days)`,
coalesce(last_known_survival_status, '') as `Last Known Survival Status`
Order by participant_id limit 100
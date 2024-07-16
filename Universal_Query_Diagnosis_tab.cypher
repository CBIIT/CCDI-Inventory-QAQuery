MATCH (p:participant)-->(st:study)
where st.phs_accession in ['']
optional MATCH (p)<-[:of_sample]-(sm1:sample)<--(cl)<--(sm2:sample)
WHERE (cl: cell_line or cl: pdx)
optional Match (sm2)<--(file)
WHERE (file: sequencing_file OR file:pathology_file OR file:methylation_array_file OR file:single_cell_sequencing_file OR file:cytogenomic_file) 
with p, case COLLECT(distinct sm1) when [] then []
              else COLLECT(DISTINCT {
                      sample_anatomic_site: sm1.anatomic_site,
                      participant_age_at_collection: sm1.participant_age_at_collection,
                      sample_tumor_status: sm1.sample_tumor_status,
                      tumor_classification: sm1.tumor_classification,
                      assay_method: CASE LABELS(file)[0]
                                WHEN 'sequencing_file' THEN 'Sequencing'
                                WHEN 'single_cell_sequencing_file' THEN 'Single Cell Sequencing'
                                WHEN 'cytogenomic_file' THEN 'Cytogenomic'
                                WHEN 'pathology_file' THEN 'Pathology imaging'
                                WHEN 'methylation_array_file' THEN 'Methylation array'
                                ELSE null END,
                      file_type: CASE LABELS(file)[0]
                                When null then null
                                else file.file_type end,
                      library_selection: CASE LABELS(file)[0]
                                    WHEN 'sequencing_file' THEN file.library_selection
                                    WHEN 'single_cell_sequencing_file' THEN file.library_selection
                                    ELSE null END,
                      library_source: CASE LABELS(file)[0]
                                    WHEN 'sequencing_file' THEN file.library_source
                                    WHEN 'single_cell_sequencing_file' THEN file.library_source
                                    ELSE null END,
                      library_strategy: CASE LABELS(file)[0]
                                    WHEN 'sequencing_file' THEN file.library_strategy
                                    WHEN 'single_cell_sequencing_file' THEN file.library_strategy
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
                                WHEN 'single_cell_sequencing_file' THEN 'Single Cell Sequencing'
                                WHEN 'cytogenomic_file' THEN 'Cytogenomic'
                                WHEN 'pathology_file' THEN 'Pathology imaging'
                                WHEN 'methylation_array_file' THEN 'Methylation array'
                                ELSE null END,
                      file_type: CASE LABELS(file)[0]
                                When null then null
                                else file.file_type end,
                      library_selection: CASE LABELS(file)[0]
                                    WHEN 'sequencing_file' THEN file.library_selection
                                    WHEN 'single_cell_sequencing_file' THEN file.library_selection
                                    ELSE null END,
                      library_source: CASE LABELS(file)[0]
                                    WHEN 'sequencing_file' THEN file.library_source
                                    WHEN 'single_cell_sequencing_file' THEN file.library_source
                                    ELSE null END,
                      library_strategy: CASE LABELS(file)[0]
                                    WHEN 'sequencing_file' THEN file.library_strategy
                                    WHEN 'single_cell_sequencing_file' THEN file.library_strategy
                                    ELSE null END
                  }) end AS sample2
with p, apoc.coll.union(sample1,sample2) as cell_line_pdx_file_filters
OPTIONAL MATCH (p)<-[:of_sample]-(sm:sample)<--(file)
WHERE (file: sequencing_file OR file:pathology_file OR file:methylation_array_file OR file:single_cell_sequencing_file OR file:cytogenomic_file)
with p, cell_line_pdx_file_filters, COLLECT(DISTINCT {
              sample_anatomic_site: sm.anatomic_site,
              participant_age_at_collection: sm.participant_age_at_collection,
              sample_tumor_status: sm.sample_tumor_status,
              tumor_classification: sm.tumor_classification,
              assay_method: CASE LABELS(file)[0]
                        WHEN 'sequencing_file' THEN 'Sequencing'
                        WHEN 'single_cell_sequencing_file' THEN 'Single Cell Sequencing'
                        WHEN 'cytogenomic_file' THEN 'Cytogenomic'
                        WHEN 'pathology_file' THEN 'Pathology imaging'
                        WHEN 'methylation_array_file' THEN 'Methylation array' END,
              file_type: file.file_type,
              library_selection: CASE LABELS(file)[0]
                            WHEN 'sequencing_file' THEN file.library_selection
                            WHEN 'single_cell_sequencing_file' THEN file.library_selection
                            ELSE null END,
              library_source: CASE LABELS(file)[0]
                            WHEN 'sequencing_file' THEN file.library_source
                            WHEN 'single_cell_sequencing_file' THEN file.library_source
                            ELSE null END,
              library_strategy: CASE LABELS(file)[0]
                            WHEN 'sequencing_file' THEN file.library_strategy
                            WHEN 'single_cell_sequencing_file' THEN file.library_strategy
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
          library_source: null,
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
          library_source: null,
          library_strategy: null
  }) as participant_radiology_file_filters
MATCH (dg:diagnosis)
MATCH (p)<-[:of_diagnosis]-(dg)
OPTIONAL MATCH (p)<-[*..4]-(file)
WHERE (file:clinical_measure_file OR file: sequencing_file OR file:pathology_file OR file:radiology_file OR file:methylation_array_file OR file:single_cell_sequencing_file OR file:cytogenomic_file)
OPTIONAL MATCH (p)<-[:of_follow_up]-(fu:follow_up)
with p, cell_line_pdx_file_filters, general_file_filters, participant_clinical_measure_file_filters,participant_radiology_file_filters, dg, file, fu order by fu.follow_up_id desc, fu.age_at_follow_up desc
OPTIONAL MATCH (st:study)<-[:of_participant]-(p)
OPTIONAL MATCH (st)<-[:of_study_personnel]-(stp:study_personnel)
OPTIONAL MATCH (st)<-[:of_study_funding]-(stf:study_funding)
WITH p, cell_line_pdx_file_filters, general_file_filters, participant_clinical_measure_file_filters,participant_radiology_file_filters, file, fu, st, stf, stp, dg
with DISTINCT
  dg.id as id,
  dg.diagnosis_classification as diagnosis_classification,
  dg.disease_phase as disease_phase,
  dg.diagnosis_classification_system as diagnosis_classification_system,
  dg.diagnosis_verification_status as diagnosis_verification_status,
  dg.diagnosis_basis as diagnosis_basis,
  dg.diagnosis_comment as diagnosis_comment,
  dg.anatomic_site as diagnosis_anatomic_site,
  dg.age_at_diagnosis as age_at_diagnosis,
  p.participant_id as participant_id,
  apoc.text.split(p.race, ';') as race,
  p.sex_at_birth as sex_at_birth,
  apoc.text.split(p.ethnicity, ';') as ethnicity,
  st.study_id as study_id,
  st.phs_accession as phs_accession,
  st.study_acronym as study_acronym,
  st.study_short_title as study_short_title,
  head(collect(distinct fu.vital_status)) as last_vital_status,
  COLLECT(DISTINCT fu.vital_status) as vital_status,
  apoc.coll.union(cell_line_pdx_file_filters, general_file_filters) + participant_clinical_measure_file_filters + participant_radiology_file_filters AS sample_file_filters,
  COLLECT(DISTINCT stf.grant_id) as grant_id,
  COLLECT(DISTINCT stp.institution) as institution
where ANY(element IN [''] WHERE element IN grant_id) and ANY(element IN [''] WHERE element IN institution) and study_acronym in [''] and study_short_title in ['']
with id, participant_id, phs_accession, sex_at_birth, race, ethnicity, diagnosis_classification, diagnosis_classification_system, diagnosis_verification_status, diagnosis_basis, diagnosis_comment, disease_phase, diagnosis_anatomic_site, age_at_diagnosis, last_vital_status, vital_status, sample_file_filters
where participant_id in [''] and sex_at_birth in [''] and ANY(element IN [''] WHERE element IN race) and ANY(element IN [''] WHERE element IN ethnicity)
with id, participant_id, phs_accession, diagnosis_classification, diagnosis_classification_system, diagnosis_verification_status, diagnosis_basis, diagnosis_comment, disease_phase, diagnosis_anatomic_site, age_at_diagnosis, last_vital_status, vital_status, sample_file_filters
where age_at_diagnosis >= [''] and age_at_diagnosis <= [''] and diagnosis_anatomic_site in [''] and diagnosis_classification in [''] and diagnosis_classification_system in [''] and diagnosis_verification_status in [''] and diagnosis_basis in [''] and disease_phase in ['']
with id, participant_id, phs_accession, diagnosis_classification, diagnosis_classification_system, diagnosis_verification_status, diagnosis_basis, diagnosis_comment, disease_phase, diagnosis_anatomic_site, age_at_diagnosis, last_vital_status, vital_status, sample_file_filters
where ANY(element IN [''] WHERE element IN vital_status) 
unwind sample_file_filters as sample_file_filter
with id, participant_id, phs_accession, diagnosis_classification, diagnosis_classification_system, diagnosis_verification_status, diagnosis_basis, diagnosis_comment, disease_phase, diagnosis_anatomic_site, age_at_diagnosis, last_vital_status, sample_file_filter
where sample_file_filter.participant_age_at_collection >= [''] and sample_file_filter.participant_age_at_collection <= [''] and sample_file_filter.sample_anatomic_site in [''] and sample_file_filter.sample_tumor_status in [''] and sample_file_filter.tumor_classification in [''] and sample_file_filter.assay_method in [''] and sample_file_filter.file_type in [''] and sample_file_filter.library_selection in [''] and sample_file_filter.library_source in [''] and sample_file_filter.library_strategy in ['']
with distinct id, participant_id, phs_accession, diagnosis_classification, diagnosis_classification_system, diagnosis_verification_status, diagnosis_basis, diagnosis_comment, disease_phase, diagnosis_anatomic_site, age_at_diagnosis, last_vital_status
return
coalesce(participant_id, '') as `Participant ID`,
coalesce(phs_accession, '') as `Study ID`,
coalesce(diagnosis_classification, '') as `Diagnosis`,
coalesce(diagnosis_classification_system, '') as `Diagnosis Classification System`,
coalesce(diagnosis_verification_status, '') as `Diagnosis Verification Status`,
coalesce(diagnosis_basis, '') as `Diagnosis Basis`,
coalesce(diagnosis_comment, '') as `Diagnosis Comment`,
coalesce(disease_phase, '') as `Disease Phase`,
coalesce(diagnosis_anatomic_site, '') as `Anatomic Site`,
case age_at_diagnosis when -999 then 'Not Reported' else coalesce(age_at_diagnosis, '') end as `Age at diagnosis (days)`,
coalesce(last_vital_status, '') as `Vital Status`
Order by participant_id limit 100
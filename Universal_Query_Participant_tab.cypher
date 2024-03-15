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
OPTIONAL MATCH (p)<-[*..4]-(file)
WHERE (file:clinical_measure_file OR file: sequencing_file OR file:pathology_file OR file:radiology_file OR file:methylation_array_file OR file:single_cell_sequencing_file OR file:cytogenomic_file)
OPTIONAL MATCH (p)<-[:of_diagnosis]-(dg:diagnosis)
OPTIONAL MATCH (p)<-[:of_follow_up]-(fu:follow_up)
OPTIONAL MATCH (st:study)<-[:of_participant]-(p)
OPTIONAL MATCH (st)<-[:of_study_personnel]-(stp:study_personnel)
OPTIONAL MATCH (st)<-[:of_study_funding]-(stf:study_funding)
WITH p, cell_line_pdx_file_filters, general_file_filters, participant_clinical_measure_file_filters, participant_radiology_file_filters, file, fu, st, stf, stp, dg
with DISTINCT
  p.id as id,
  p.participant_id as participant_id,
  apoc.text.split(p.race, ';') as race,
  p.race as race_str,
  p.sex_at_birth as sex_at_birth,
  p.ethnicity as ethnicity_str,
  apoc.text.split(p.ethnicity, ';') as ethnicity,
  p.alternate_participant_id as alternate_participant_id,
  COLLECT(DISTINCT {
      age_at_diagnosis: dg.age_at_diagnosis,
      diagnosis_anatomic_site: dg.anatomic_site,
      disease_phase: dg.disease_phase,
      diagnosis_classification_system: dg.diagnosis_classification_system,
      diagnosis_verification_status: dg.diagnosis_verification_status,
      diagnosis_basis: dg.diagnosis_basis,
      diagnosis_comment: dg.diagnosis_comment,
      diagnosis_classification: dg.diagnosis_classification
  }) AS diagnosis_filters,
  COLLECT(DISTINCT fu.vital_status) as vital_status,
  apoc.coll.union(cell_line_pdx_file_filters, general_file_filters) + participant_clinical_measure_file_filters + participant_radiology_file_filters AS sample_file_filters,
  st.study_id as study_id,
  st.phs_accession as phs_accession,
  COLLECT(DISTINCT stf.grant_id) as grant_id,
  COLLECT(DISTINCT stp.institution) as institution,
  st.study_acronym as study_acronym,
  st.study_short_title as study_short_title
  where ANY(element IN [''] WHERE element IN grant_id) and ANY(element IN [''] WHERE element IN institution) and study_acronym in [''] and study_short_title in ['']
  with id, participant_id, phs_accession, sex_at_birth, race_str, ethnicity_str, race, ethnicity, alternate_participant_id, diagnosis_filters, vital_status, sample_file_filters
  where participant_id in [''] and sex_at_birth in [''] and ANY(element IN [''] WHERE element IN race) and ANY(element IN [''] WHERE element IN ethnicity)
  unwind diagnosis_filters as diagnosis_filter
  with id, participant_id, phs_accession, sex_at_birth, race_str, ethnicity_str, alternate_participant_id, diagnosis_filter, vital_status, sample_file_filters
  where diagnosis_filter.age_at_diagnosis >= [''] and diagnosis_filter.age_at_diagnosis <= [''] and diagnosis_filter.diagnosis_anatomic_site in [''] and diagnosis_filter.diagnosis_classification in [''] and diagnosis_filter.diagnosis_classification_system in [''] and diagnosis_filter.diagnosis_verification_status in [''] and diagnosis_filter.diagnosis_basis in [''] and diagnosis_filter.disease_phase in ['']
  with id, participant_id, phs_accession, sex_at_birth, race_str, ethnicity_str, alternate_participant_id, vital_status, sample_file_filters
  where ANY(element IN [''] WHERE element IN vital_status)
  unwind sample_file_filters as sample_file_filter
  with id, participant_id, phs_accession, sex_at_birth, race_str, ethnicity_str, alternate_participant_id, sample_file_filter
  where sample_file_filter.participant_age_at_collection >= [''] and sample_file_filter.participant_age_at_collection <= [''] and sample_file_filter.sample_anatomic_site in [''] and sample_file_filter.sample_tumor_status in [''] and sample_file_filter.tumor_classification in [''] and sample_file_filter.assay_method in [''] and sample_file_filter.file_type in [''] and sample_file_filter.library_selection in [''] and sample_file_filter.library_source in [''] and sample_file_filter.library_strategy in ['']
  with distinct id, participant_id, phs_accession, sex_at_birth, race_str, ethnicity_str, alternate_participant_id
  return
  coalesce(participant_id, '') AS `Participant ID`,
  coalesce(phs_accession, '') AS `Study ID`,
  coalesce(sex_at_birth, '') AS `Sex` ,
  coalesce(race_str, '') AS `Race`,
  coalesce(ethnicity_str, '') AS `Ethnicity` ,
  coalesce(alternate_participant_id, '') AS `Alternate ID`
  Order by participant_id Limit 100
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
  with id, participant_id, phs_accession, sex_at_birth, race_str, ethnicity_str, alternate_participant_id, diagnosis_filters, vital_status, sample_file_filters
  where participant_id in [''] and sex_at_birth in [''] and ANY(element IN [''] WHERE element IN apoc.text.split(race_str, ';')) and ANY(element IN [''] WHERE element IN apoc.text.split(ethnicity_str, ';'))
  unwind diagnosis_filters as diagnosis_filter
  with id, participant_id, phs_accession, sex_at_birth, race_str, ethnicity_str, alternate_participant_id, diagnosis_filter, vital_status, sample_file_filters
  where diagnosis_filter.age_at_diagnosis >= [''] and diagnosis_filter.age_at_diagnosis <= [''] and diagnosis_filter.diagnosis_anatomic_site in [''] and diagnosis_filter.diagnosis_classification in [''] and diagnosis_filter.diagnosis_classification_system in [''] and diagnosis_filter.diagnosis_verification_status in [''] and diagnosis_filter.diagnosis_basis in [''] and diagnosis_filter.disease_phase in ['']
  with id, participant_id, phs_accession, sex_at_birth, race_str, ethnicity_str, alternate_participant_id, vital_status, sample_file_filters
  where ANY(element IN [''] WHERE element IN vital_status)
  unwind sample_file_filters as sample_file_filter
  with id, participant_id, phs_accession, sex_at_birth, race_str, ethnicity_str, alternate_participant_id, sample_file_filter
  where sample_file_filter.participant_age_at_collection >= [''] and sample_file_filter.participant_age_at_collection <= [''] and sample_file_filter.sample_anatomic_site in [''] and sample_file_filter.sample_tumor_status in [''] and sample_file_filter.tumor_classification in [''] and sample_file_filter.assay_method in [''] and sample_file_filter.file_type in [''] and sample_file_filter.library_selection in [''] and sample_file_filter.library_source in [''] and sample_file_filter.library_strategy in ['']
  with distinct id, participant_id, phs_accession, sex_at_birth, race_str, ethnicity_str, alternate_participant_id
  with distinct phs_accession as study_ids
  MATCH (st:study)<-[:of_participant]-(p:participant)
  where st.study_id = study_ids
  with st, count(p) as num_p
  MATCH (st:study)<-[:of_participant]-(participant)<-[:of_diagnosis]-(dg:diagnosis)
  with st, num_p, dg.diagnosis_classification as dg_cancers, count(dg.diagnosis_classification) as num_cancers
  ORDER BY num_cancers desc
  with st, num_p, collect(dg_cancers + ' (' + toString(num_cancers) + ')') as cancers
  MATCH (st)<-[:of_participant]-(pa:participant)<-[:of_diagnosis]-(diag:diagnosis)
  with st, num_p, cancers, diag.anatomic_site as dg_sites, count(diag.anatomic_site) as num_sites
  ORDER BY num_sites desc
  with st, num_p, cancers, collect(dg_sites + ' (' + toString(num_sites) + ')') as sites
  MATCH (st)<-[*..5]-(fl)
  WHERE (fl:clinical_measure_file OR fl: sequencing_file OR fl:pathology_file OR fl:radiology_file OR fl:methylation_array_file OR fl:single_cell_sequencing_file OR fl:cytogenomic_file)
  with st, num_p, cancers, sites, fl.file_type as ft, count(fl.file_type) as num_ft
  ORDER BY num_ft desc
  with st, num_p, cancers, sites, collect(ft + ' (' + toString(num_ft) + ')') as file_types, sum(num_ft) as num_files
  OPTIONAL MATCH (st)<-[:of_participant|of_cell_line|of_pdx]-(pcp)<-[:of_sample]-(sm1:sample)
  WHERE (pcp:participant or pcp:cell_line or pcp:pdx)
  WITH st, num_p, cancers, sites, file_types, num_files, count(distinct sm1.sample_id) as num_samples_1
  OPTIONAL MATCH (st)<-[:of_participant]-(participant)<-[:of_sample]-(sm1:sample)<--(cp)<--(sm2:sample)
  WHERE (cp:cell_line or cp:pdx)
  WITH st, num_p, cancers, sites, file_types, num_files, num_samples_1, count(distinct sm2.sample_id) as num_samples_2
  WITH st, num_p, cancers, sites, file_types, num_files, num_samples_1 + num_samples_2 as num_samples
  MATCH (st)<-[*..5]-(file)
  WHERE (file:clinical_measure_file OR file: sequencing_file OR file:pathology_file OR file:radiology_file OR file:methylation_array_file OR file:single_cell_sequencing_file OR file:cytogenomic_file)
  OPTIONAL MATCH (st)<-[:of_publication]-(pub:publication)
  OPTIONAL MATCH (st)<-[:of_study_personnel]-(stp:study_personnel)
  WHERE stp.personnel_type = 'PI'
  OPTIONAL MATCH (st)<-[:of_study_funding]-(stf:study_funding)
  WITH st, num_p, cancers, sites, file_types, num_files, num_samples, file.id as file_id, stf, stp, pub
  with DISTINCT
      st.id as id,
      st.study_id as study_id,
      apoc.text.join(COLLECT(DISTINCT stf.grant_id), ';') as grant_id,
      apoc.text.join(COLLECT(DISTINCT pub.pubmed_id), ';') as pubmed_ids,
      st.phs_accession as phs_accession,
      st.study_short_title as study_short_title,
      st.study_acronym as study_acronym,
      apoc.text.join(COLLECT(DISTINCT stp.personnel_name), ';') as PIs,
      num_p as num_of_participants,
      cancers as diagnosis_cancer,
      sites as diagnosis_anatomic_site,
      file_types as file_types,
      num_samples as num_of_samples,
      num_files as num_of_files
  RETURN DISTINCT
  study_short_title as `Study Short Title`,
  study_id as `Study ID`,
  CASE WHEN size(diagnosis_cancer) > 5 THEN '="' + apoc.text.join(apoc.coll.remove(diagnosis_cancer, 5, 10000), '"&CHAR(10)&"') + '"&CHAR(10)&"Read More"'  else '="' + apoc.text.join(diagnosis_cancer, '"&CHAR(10)&"') + '"' END as `Diagnosis (Top 5)`,
  CASE WHEN size(diagnosis_anatomic_site) > 5 THEN '="' + apoc.text.join(apoc.coll.remove(diagnosis_anatomic_site, 5, 10000), '"&CHAR(10)&"') + '"&CHAR(10)&"Read More"'  else '="' + apoc.text.join(diagnosis_anatomic_site, '"&CHAR(10)&"') + '"' END as `Diagnosis Anatomic Site (Top 5)`,
  num_of_participants as `Number of Participants`,
  num_of_samples as `Number of Samples`,
  num_of_files as `Number of Files`,
  CASE WHEN size(file_types) > 5 THEN '="' + apoc.text.join(apoc.coll.remove(file_types, 5, 10000), '"&CHAR(10)&"') + '"&CHAR(10)&"Read More"'  else '="' + apoc.text.join(file_types, '"&CHAR(10)&"') + '"' END as `File Type (Top 5)`,
  pubmed_ids as `PubMed ID`,
  PIs as `Principal Investigator(s)`,
  grant_id as `Grant ID`
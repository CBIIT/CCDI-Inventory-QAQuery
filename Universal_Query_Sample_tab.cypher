MATCH (st:study)
where st.phs_accession in ['']
with st
call {
  with st
  MATCH (sm:sample)
        OPTIONAL MATCH (p:participant)<-[*..3]-(sm)
        optional match (sm)<-[*..3]-(file)
        WHERE (file: sequencing_file OR file:pathology_file OR file:methylation_array_file OR file:single_cell_sequencing_file OR file:cytogenomic_file)
        MATCH (st)<-[:of_participant]-(p)
        OPTIONAL MATCH (p)<-[:of_diagnosis]-(dg:diagnosis)
        OPTIONAL MATCH (p)<-[:of_follow_up]-(fu:follow_up)
        OPTIONAL MATCH (st)<-[:of_study_personnel]-(stp:study_personnel)
        OPTIONAL MATCH (st)<-[:of_study_funding]-(stf:study_funding)
        WITH file, fu, p, st, sm, stf, stp, dg
        RETURN DISTINCT
          sm.id as id,
          p.id as pid,
          sm.sample_id as sample_id,
          p.participant_id as participant_id,
          apoc.text.split(p.race, ';') as race,
          p.sex_at_birth as sex_at_birth,
          apoc.text.split(p.ethnicity, ';') as ethnicity,
          sm.anatomic_site as sample_anatomic_site,
          sm.diagnosis_classification as sample_diagnosis_classification,
          sm.diagnosis_classification_system as sample_diagnosis_classification_system,
          sm.diagnosis_verification_status as sample_diagnosis_verification_status,
          sm.diagnosis_basis as sample_diagnosis_basis,
          sm.diagnosis_comment as sample_diagnosis_comment,
          sm.participant_age_at_collection as participant_age_at_collection,
          sm.sample_tumor_status as sample_tumor_status,
          sm.tumor_classification as tumor_classification,
          st.study_id as study_id,
          st.phs_accession as phs_accession,
          st.study_acronym as study_acronym,
          st.study_short_title as study_short_title,
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
          COLLECT(DISTINCT {
              assay_method: CASE LABELS(file)[0]
                        WHEN 'sequencing_file' THEN 'Sequencing'
                        WHEN 'single_cell_sequencing_file' THEN 'Single Cell Sequencing'
                        WHEN 'cytogenomic_file' THEN 'Cytogenomic'
                        WHEN 'pathology_file' THEN 'Pathology imaging'
                        WHEN 'methylation_array_file' THEN 'Methylation array' 
                        ELSE null END,
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
          }) AS file_filters,
          COLLECT(DISTINCT stf.grant_id) as grant_id,
          COLLECT(DISTINCT stp.institution) as institution
        union all
        with st
        MATCH (sm:sample)
        MATCH (st)<-[:of_cell_line|of_pdx]-(cl)<--(sm)
        Where (cl:cell_line or cl:pdx)
        optional Match (sm)<--(file)
        WHERE (file: sequencing_file OR file:pathology_file OR file:methylation_array_file OR file:single_cell_sequencing_file OR file:cytogenomic_file)
        MATCH (st)<-[:of_participant]-(p:participant)
        OPTIONAL MATCH (p)<-[:of_diagnosis]-(dg:diagnosis)
        OPTIONAL MATCH (p)<-[:of_follow_up]-(fu:follow_up)
        OPTIONAL MATCH (st)<-[:of_study_personnel]-(stp:study_personnel)
        OPTIONAL MATCH (st)<-[:of_study_funding]-(stf:study_funding)
        WITH sm, file, fu, st, stf, stp, dg
        RETURN DISTINCT
          sm.id as id,
          null as pid,
          sm.sample_id as sample_id,
          null as participant_id,
          null as race,
          null as sex_at_birth,
          null as ethnicity,
          sm.anatomic_site as sample_anatomic_site,
          sm.diagnosis_classification as sample_diagnosis_classification,
          sm.diagnosis_classification_system as sample_diagnosis_classification_system,
          sm.diagnosis_verification_status as sample_diagnosis_verification_status,
          sm.diagnosis_basis as sample_diagnosis_basis,
          sm.diagnosis_comment as sample_diagnosis_comment,
          sm.participant_age_at_collection as participant_age_at_collection,
          sm.sample_tumor_status as sample_tumor_status,
          sm.tumor_classification as tumor_classification,
          st.study_id as study_id,
          st.phs_accession as phs_accession,
          st.study_acronym as study_acronym,
          st.study_short_title as study_short_title,
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
          CASE COLLECT(file) WHEN [] THEN []
                    ELSE COLLECT(DISTINCT {
                        assay_method: CASE LABELS(file)[0]
                                  WHEN 'sequencing_file' THEN 'Sequencing'
                                  WHEN 'single_cell_sequencing_file' THEN 'Single Cell Sequencing'
                                  WHEN 'cytogenomic_file' THEN 'Cytogenomic'
                                  WHEN 'pathology_file' THEN 'Pathology imaging'
                                  WHEN 'methylation_array_file' THEN 'Methylation array' 
                                  ELSE null END,
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
                    }) END AS file_filters,
          COLLECT(DISTINCT stf.grant_id) as grant_id,
          COLLECT(DISTINCT stp.institution) as institution
}
with id, sample_id, participant_id, study_id, sex_at_birth, race, ethnicity, sample_anatomic_site, sample_diagnosis_classification, sample_diagnosis_classification_system, sample_diagnosis_verification_status, sample_diagnosis_basis, sample_diagnosis_comment, participant_age_at_collection, sample_tumor_status, tumor_classification, diagnosis_filters, vital_status, file_filters, phs_accession, grant_id, institution, study_acronym, study_short_title
where ANY(element IN [''] WHERE element IN grant_id) and ANY(element IN [''] WHERE element IN institution) and study_acronym in [''] and study_short_title in ['']
with id, sample_id, participant_id, study_id, sex_at_birth, race, ethnicity, sample_anatomic_site, sample_diagnosis_classification, sample_diagnosis_classification_system, sample_diagnosis_verification_status, sample_diagnosis_basis, sample_diagnosis_comment, participant_age_at_collection, sample_tumor_status, tumor_classification, diagnosis_filters, vital_status, file_filters, phs_accession
where participant_id in [''] and sex_at_birth in [''] and ANY(element IN [''] WHERE element IN race) and ANY(element IN [''] WHERE element IN ethnicity)
unwind diagnosis_filters as diagnosis_filter
with id, sample_id, participant_id, study_id, sample_anatomic_site, sample_diagnosis_classification, sample_diagnosis_classification_system, sample_diagnosis_verification_status, sample_diagnosis_basis, sample_diagnosis_comment, participant_age_at_collection, sample_tumor_status, tumor_classification, diagnosis_filter, vital_status, file_filters, phs_accession
where diagnosis_filter.age_at_diagnosis >= [''] and diagnosis_filter.age_at_diagnosis <= [''] and diagnosis_filter.diagnosis_anatomic_site in [''] and diagnosis_filter.diagnosis_classification in [''] and diagnosis_filter.diagnosis_classification_system in [''] and diagnosis_filter.diagnosis_verification_status in [''] and diagnosis_filter.diagnosis_basis in [''] and diagnosis_filter.disease_phase in ['']
with id, sample_id, participant_id, study_id, sample_anatomic_site, sample_diagnosis_classification, sample_diagnosis_classification_system, sample_diagnosis_verification_status, sample_diagnosis_basis, sample_diagnosis_comment, participant_age_at_collection, sample_tumor_status, tumor_classification, vital_status, file_filters, phs_accession
where ANY(element IN [''] WHERE element IN vital_status) 
with id, sample_id, participant_id, study_id, sample_anatomic_site, sample_diagnosis_classification, sample_diagnosis_classification_system, sample_diagnosis_verification_status, sample_diagnosis_basis, sample_diagnosis_comment, participant_age_at_collection, sample_tumor_status, tumor_classification, file_filters, phs_accession
where participant_age_at_collection >= [''] and participant_age_at_collection <= [''] and sample_anatomic_site in [''] and sample_tumor_status in [''] and tumor_classification in ['']
unwind file_filters as file_filter
with id, sample_id, participant_id, study_id, sample_anatomic_site, sample_diagnosis_classification, sample_diagnosis_classification_system, sample_diagnosis_verification_status, sample_diagnosis_basis, sample_diagnosis_comment, participant_age_at_collection, sample_tumor_status, tumor_classification, file_filter, phs_accession
where file_filter.assay_method in [''] and file_filter.file_type in [''] and file_filter.library_selection in [''] and file_filter.library_source in [''] and file_filter.library_strategy in ['']
with distinct id, sample_id, participant_id, study_id, sample_anatomic_site, sample_diagnosis_classification, sample_diagnosis_classification_system, sample_diagnosis_verification_status, sample_diagnosis_basis, sample_diagnosis_comment, participant_age_at_collection, sample_tumor_status, tumor_classification, phs_accession
RETURN DISTINCT
          sample_id as `Sample ID`,
          participant_id as `Participant ID`,
          study_id as `Study ID`,
          sample_anatomic_site as `Anatomic Site`,
          case participant_age_at_collection when -999 then 'Not Reported' else coalesce(participant_age_at_collection, '') end as `Age at Sample Collection`,
          coalesce(sample_diagnosis_classification, '') as `Diagnosis`,
          coalesce(sample_diagnosis_classification_system, '') as `Diagnosis Classification System`,
          coalesce(sample_diagnosis_verification_status, '') as `Diagnosis Verification Status`,
          coalesce(sample_diagnosis_basis, '') as `Diagnosis Basis`,
          coalesce(sample_diagnosis_comment, '') as `Diagnosis Comment`,
          sample_tumor_status as `Sample Tumor Status`,
          tumor_classification as `Sample Tumor Classification`
Order by sample_id Limit 100


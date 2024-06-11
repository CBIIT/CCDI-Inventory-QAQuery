Match (st:study)
where st.phs_accession in ['']
with st
Call {
with st
MATCH (file:clinical_measure_file)
MATCH (p:participant)-[:of_clinical_measure_file]-(file)
MATCH (st)<-[:of_participant]-(p)
OPTIONAL MATCH (p)<-[:of_sample]-(sm:sample)
OPTIONAL MATCH (st)<-[:of_publication]-(pub:publication)
OPTIONAL MATCH (p)<-[:of_diagnosis]-(dg:diagnosis)
OPTIONAL MATCH (p)<-[:of_follow_up]-(fu:follow_up)
OPTIONAL MATCH (st)<-[:of_study_personnel]-(stp:study_personnel)
OPTIONAL MATCH (st)<-[:of_study_funding]-(stf:study_funding)
UNWIND apoc.text.split(p.ethnicity, ';') AS ethnicities
UNWIND apoc.text.split(p.race, ';') AS races
RETURN DISTINCT
  file.id as id,
  p.id as pid,
  file.clinical_measure_file_id AS file_id,
  file.dcf_indexd_guid AS guid,
  file.file_name AS file_name,
  'Clinical data' AS file_category,
  file.file_type AS file_type,
  file.file_description AS file_description,
  file.file_size AS file_size,
  file.md5sum AS md5sum,
  st.study_id AS study_id,
  st.phs_accession as phs_accession,
  st.study_acronym as study_acronym,
  st.study_short_title as study_short_title,
  p.participant_id AS participant_id,
  null AS sample_id,
  COLLECT(DISTINCT {
      race: races,
      sex_at_birth: p.sex_at_birth,
      ethnicity: ethnicities
  }) AS participant_filters,
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
      sample_anatomic_site: sm.anatomic_site,
      participant_age_at_collection: sm.participant_age_at_collection,
      sample_tumor_status: sm.sample_tumor_status,
      tumor_classification: sm.tumor_classification
  }) AS sample_filters,
  COLLECT(DISTINCT stf.grant_id) as grant_id,
  COLLECT(DISTINCT stp.institution) as institution,      
  null AS library_selection,
  null AS library_source,
  null AS library_strategy
UNION ALL
with st
MATCH (file:methylation_array_file)
MATCH (p:participant)<-[:of_sample]-(sm1:sample)<-[*0..2]-(sm:sample)<-[:of_methylation_array_file]-(file)
MATCH (st)<-[:of_participant]-(p)
OPTIONAL MATCH (st)<-[:of_publication]-(pub:publication)
OPTIONAL MATCH (p)<-[:of_diagnosis]-(dg:diagnosis)
OPTIONAL MATCH (p)<-[:of_follow_up]-(fu:follow_up)
OPTIONAL MATCH (st)<-[:of_study_personnel]-(stp:study_personnel)
OPTIONAL MATCH (st)<-[:of_study_funding]-(stf:study_funding)
UNWIND apoc.text.split(p.ethnicity, ';') AS ethnicities
UNWIND apoc.text.split(p.race, ';') AS races
with file, p, sm1, sm, st, ethnicities, races, fu, dg, stf, stp
RETURN DISTINCT
  file.id as id,
  p.id as pid,
  file.methylation_array_file_id AS file_id,
  file.dcf_indexd_guid AS guid,
  file.file_name AS file_name,
  'Methylation array' AS file_category,
  file.file_type AS file_type,
  file.file_description AS file_description,
  file.file_size AS file_size,
  file.md5sum AS md5sum,
  st.study_id AS study_id,
  st.phs_accession as phs_accession,
  st.study_acronym as study_acronym,
  st.study_short_title as study_short_title,
  p.participant_id AS participant_id,
  CASE sm1.sample_id WHEN sm.sample_id THEN sm.sample_id
            ELSE sm1.sample_id + ',' + sm.sample_id END AS sample_id,
  COLLECT(DISTINCT {
      race: races,
      sex_at_birth: p.sex_at_birth,
      ethnicity: ethnicities
  }) AS participant_filters,
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
  CASE sm1.sample_id WHEN sm.sample_id THEN COLLECT(DISTINCT {
                              sample_anatomic_site: sm.anatomic_site,
                              participant_age_at_collection: sm.participant_age_at_collection,
                              sample_tumor_status: sm.sample_tumor_status,
                              tumor_classification: sm.tumor_classification
                          })
            ELSE apoc.coll.union(COLLECT(DISTINCT {
                              sample_anatomic_site: sm1.anatomic_site,
                              participant_age_at_collection: sm1.participant_age_at_collection,
                              sample_tumor_status: sm1.sample_tumor_status,
                              tumor_classification: sm1.tumor_classification
                          }), COLLECT(DISTINCT {
                              sample_anatomic_site: sm.anatomic_site,
                              participant_age_at_collection: sm.participant_age_at_collection,
                              sample_tumor_status: sm.sample_tumor_status,
                              tumor_classification: sm.tumor_classification
                          })) END AS sample_filters,
  COLLECT(DISTINCT stf.grant_id) as grant_id,
  COLLECT(DISTINCT stp.institution) as institution,
  null AS library_selection,
  null AS library_source,
  null AS library_strategy
UNION ALL
with st
MATCH (file:pathology_file)
MATCH (p:participant)<-[:of_sample]-(sm1:sample)<-[*0..2]-(sm:sample)<-[:of_pathology_file]-(file)
MATCH (st)<-[:of_participant]-(p)
OPTIONAL MATCH (st)<-[:of_publication]-(pub:publication)
OPTIONAL MATCH (p)<-[:of_diagnosis]-(dg:diagnosis)
OPTIONAL MATCH (p)<-[:of_follow_up]-(fu:follow_up)
OPTIONAL MATCH (st)<-[:of_study_personnel]-(stp:study_personnel)
OPTIONAL MATCH (st)<-[:of_study_funding]-(stf:study_funding)
UNWIND apoc.text.split(p.ethnicity, ';') AS ethnicities
UNWIND apoc.text.split(p.race, ';') AS races
with file, p, sm1, sm, st, ethnicities, races, fu, dg, stf, stp
RETURN DISTINCT
  file.id as id,
  p.id as pid,
  file.pathology_file_id AS file_id,
  file.dcf_indexd_guid AS guid,
  file.file_name AS file_name,
  'Pathology imaging' AS file_category,
  file.file_type AS file_type,
  file.file_description AS file_description,
  file.file_size AS file_size,
  file.md5sum AS md5sum,
  st.study_id AS study_id,
  st.phs_accession as phs_accession,
  st.study_acronym as study_acronym,
  st.study_short_title as study_short_title,
  p.participant_id AS participant_id,
  CASE sm1.sample_id WHEN sm.sample_id THEN sm.sample_id
            ELSE sm1.sample_id + ',' + sm.sample_id END AS sample_id,
  COLLECT(DISTINCT {
      race: races,
      sex_at_birth: p.sex_at_birth,
      ethnicity: ethnicities
  }) AS participant_filters,
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
  CASE sm1.sample_id WHEN sm.sample_id THEN COLLECT(DISTINCT {
                              sample_anatomic_site: sm.anatomic_site,
                              participant_age_at_collection: sm.participant_age_at_collection,
                              sample_tumor_status: sm.sample_tumor_status,
                              tumor_classification: sm.tumor_classification
                          })
            ELSE apoc.coll.union(COLLECT(DISTINCT {
                              sample_anatomic_site: sm1.anatomic_site,
                              participant_age_at_collection: sm1.participant_age_at_collection,
                              sample_tumor_status: sm1.sample_tumor_status,
                              tumor_classification: sm1.tumor_classification
                          }), COLLECT(DISTINCT {
                              sample_anatomic_site: sm.anatomic_site,
                              participant_age_at_collection: sm.participant_age_at_collection,
                              sample_tumor_status: sm.sample_tumor_status,
                              tumor_classification: sm.tumor_classification
                          })) END AS sample_filters,
  COLLECT(DISTINCT stf.grant_id) as grant_id,
  COLLECT(DISTINCT stp.institution) as institution,
  file.library_selection AS library_selection,
  file.library_source AS library_source,
  file.library_strategy AS library_strategy
UNION ALL
with st
MATCH (file:radiology_file)
MATCH (p:participant)<-[:of_radiology_file]-(file)
MATCH (st)<-[:of_participant]-(p)
OPTIONAL MATCH (p)<-[:of_sample]-(sm:sample)
OPTIONAL MATCH (st)<-[:of_publication]-(pub:publication)
OPTIONAL MATCH (p)<-[:of_diagnosis]-(dg:diagnosis)
OPTIONAL MATCH (p)<-[:of_follow_up]-(fu:follow_up)
OPTIONAL MATCH (st)<-[:of_study_personnel]-(stp:study_personnel)
OPTIONAL MATCH (st)<-[:of_study_funding]-(stf:study_funding)
UNWIND apoc.text.split(p.ethnicity, ';') AS ethnicities
UNWIND apoc.text.split(p.race, ';') AS races
RETURN DISTINCT
  file.id as id,
  p.id as pid,
  file.radiology_file_id AS file_id,
  file.dcf_indexd_guid AS guid,
  file.file_name AS file_name,
  'Radiology imaging' AS file_category,
  file.file_type AS file_type,
  file.file_description AS file_description,
  file.file_size AS file_size,
  file.md5sum AS md5sum,
  st.study_id AS study_id,
  st.phs_accession as phs_accession,
  st.study_acronym as study_acronym,
  st.study_short_title as study_short_title,
  p.participant_id AS participant_id,
  null AS sample_id,
  COLLECT(DISTINCT {
      race: races,
      sex_at_birth: p.sex_at_birth,
      ethnicity: ethnicities
  }) AS participant_filters,
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
      sample_anatomic_site: sm.anatomic_site,
      participant_age_at_collection: sm.participant_age_at_collection,
      sample_tumor_status: sm.sample_tumor_status,
      tumor_classification: sm.tumor_classification
  }) AS sample_filters,
  COLLECT(DISTINCT stf.grant_id) as grant_id,
  COLLECT(DISTINCT stp.institution) as institution,
  null AS library_selection,
  null AS library_source,
  null AS library_strategy
UNION ALL
with st
MATCH (file:single_cell_sequencing_file)
MATCH (p:participant)<-[:of_sample]-(sm1:sample)<-[*0..2]-(sm:sample)<-[:of_single_cell_sequencing_file]-(file)
MATCH (st)<-[:of_participant]-(p)
OPTIONAL MATCH (st)<-[:of_publication]-(pub:publication)
OPTIONAL MATCH (p)<-[:of_diagnosis]-(dg:diagnosis)
OPTIONAL MATCH (p)<-[:of_follow_up]-(fu:follow_up)
OPTIONAL MATCH (st)<-[:of_study_personnel]-(stp:study_personnel)
OPTIONAL MATCH (st)<-[:of_study_funding]-(stf:study_funding)
UNWIND apoc.text.split(p.ethnicity, ';') AS ethnicities
UNWIND apoc.text.split(p.race, ';') AS races
with file, p, sm1, sm, st, ethnicities, races, fu, dg, stf, stp
RETURN DISTINCT
  file.id as id,
  p.id as pid,
  file.single_cell_sequencing_file_id AS file_id,
  file.dcf_indexd_guid AS guid,
  file.file_name AS file_name,
  'Single Cell Sequencing' AS file_category,
  file.file_type AS file_type,
  file.file_description AS file_description,
  file.file_size AS file_size,
  file.md5sum AS md5sum,
  st.study_id AS study_id,
  st.phs_accession as phs_accession,
  st.study_acronym as study_acronym,
  st.study_short_title as study_short_title,
  p.participant_id AS participant_id,
  CASE sm1.sample_id WHEN sm.sample_id THEN sm.sample_id
            ELSE sm1.sample_id + ',' + sm.sample_id END AS sample_id,
  COLLECT(DISTINCT {
      race: races,
      sex_at_birth: p.sex_at_birth,
      ethnicity: ethnicities
  }) AS participant_filters,
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
  CASE sm1.sample_id WHEN sm.sample_id THEN COLLECT(DISTINCT {
                              sample_anatomic_site: sm.anatomic_site,
                              participant_age_at_collection: sm.participant_age_at_collection,
                              sample_tumor_status: sm.sample_tumor_status,
                              tumor_classification: sm.tumor_classification
                          })
            ELSE apoc.coll.union(COLLECT(DISTINCT {
                              sample_anatomic_site: sm1.anatomic_site,
                              participant_age_at_collection: sm1.participant_age_at_collection,
                              sample_tumor_status: sm1.sample_tumor_status,
                              tumor_classification: sm1.tumor_classification
                          }), COLLECT(DISTINCT {
                              sample_anatomic_site: sm.anatomic_site,
                              participant_age_at_collection: sm.participant_age_at_collection,
                              sample_tumor_status: sm.sample_tumor_status,
                              tumor_classification: sm.tumor_classification
                          })) END AS sample_filters,
  COLLECT(DISTINCT stf.grant_id) as grant_id,
  COLLECT(DISTINCT stp.institution) as institution,
  file.library_selection AS library_selection,
  file.library_source AS library_source,
  file.library_strategy AS library_strategy
UNION ALL
with st
MATCH (file:sequencing_file)
MATCH (p:participant)<-[:of_sample]-(sm1:sample)<-[*0..2]-(sm:sample)<-[:of_sequencing_file]-(file)
MATCH (st)<-[:of_participant]-(p)
OPTIONAL MATCH (st)<-[:of_publication]-(pub:publication)
OPTIONAL MATCH (p)<-[:of_diagnosis]-(dg:diagnosis)
OPTIONAL MATCH (p)<-[:of_follow_up]-(fu:follow_up)
OPTIONAL MATCH (st)<-[:of_study_personnel]-(stp:study_personnel)
OPTIONAL MATCH (st)<-[:of_study_funding]-(stf:study_funding)
UNWIND apoc.text.split(p.ethnicity, ';') AS ethnicities
UNWIND apoc.text.split(p.race, ';') AS races
with file, p, sm1, sm, st, ethnicities, races, fu, dg, stf, stp
RETURN DISTINCT
  file.id as id,
  p.id as pid,
  file.sequencing_file_id AS file_id,
  file.dcf_indexd_guid AS guid,
  file.file_name AS file_name,
  'Sequencing' AS file_category,
  file.file_type AS file_type,
  file.file_description AS file_description,
  file.file_size AS file_size,
  file.md5sum AS md5sum,
  st.study_id AS study_id,
  st.phs_accession as phs_accession,
  st.study_acronym as study_acronym,
  st.study_short_title as study_short_title,
  p.participant_id AS participant_id,
  CASE sm1.sample_id WHEN sm.sample_id THEN sm.sample_id
            ELSE sm1.sample_id + ',' + sm.sample_id END AS sample_id,
  COLLECT(DISTINCT {
      race: races,
      sex_at_birth: p.sex_at_birth,
      ethnicity: ethnicities
  }) AS participant_filters,
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
  CASE sm1.sample_id WHEN sm.sample_id THEN COLLECT(DISTINCT {
                              sample_anatomic_site: sm.anatomic_site,
                              participant_age_at_collection: sm.participant_age_at_collection,
                              sample_tumor_status: sm.sample_tumor_status,
                              tumor_classification: sm.tumor_classification
                          })
            ELSE apoc.coll.union(COLLECT(DISTINCT {
                              sample_anatomic_site: sm1.anatomic_site,
                              participant_age_at_collection: sm1.participant_age_at_collection,
                              sample_tumor_status: sm1.sample_tumor_status,
                              tumor_classification: sm1.tumor_classification
                          }), COLLECT(DISTINCT {
                              sample_anatomic_site: sm.anatomic_site,
                              participant_age_at_collection: sm.participant_age_at_collection,
                              sample_tumor_status: sm.sample_tumor_status,
                              tumor_classification: sm.tumor_classification
                          })) END AS sample_filters,
  COLLECT(DISTINCT stf.grant_id) as grant_id,
  COLLECT(DISTINCT stp.institution) as institution,
  file.library_selection AS library_selection,
  file.library_source AS library_source,
  file.library_strategy AS library_strategy
UNION ALL
with st
MATCH (file:cytogenomic_file)
MATCH (p:participant)<-[:of_sample]-(sm1:sample)<-[*0..2]-(sm:sample)<-[:of_cytogenomic_file]-(file)
MATCH (st)<-[:of_participant]-(p)
OPTIONAL MATCH (st)<-[:of_publication]-(pub:publication)
OPTIONAL MATCH (p)<-[:of_diagnosis]-(dg:diagnosis)
OPTIONAL MATCH (p)<-[:of_follow_up]-(fu:follow_up)
OPTIONAL MATCH (st)<-[:of_study_personnel]-(stp:study_personnel)
OPTIONAL MATCH (st)<-[:of_study_funding]-(stf:study_funding)
UNWIND apoc.text.split(p.ethnicity, ';') AS ethnicities
UNWIND apoc.text.split(p.race, ';') AS races
with file, p, sm1, sm, st, ethnicities, races, fu, dg, stf, stp
RETURN DISTINCT
  file.id as id,
  p.id as pid,
  file.cytogenomic_file_id AS file_id,
  file.dcf_indexd_guid AS guid,
  file.file_name AS file_name,
  'Cytogenomic' AS file_category,
  file.file_type AS file_type,
  file.file_description AS file_description,
  file.file_size AS file_size,
  file.md5sum AS md5sum,
  st.study_id AS study_id,
  st.phs_accession as phs_accession,
  st.study_acronym as study_acronym,
  st.study_short_title as study_short_title,
  p.participant_id AS participant_id,
  CASE sm1.sample_id WHEN sm.sample_id THEN sm.sample_id
            ELSE sm1.sample_id + ',' + sm.sample_id END AS sample_id,
  COLLECT(DISTINCT {
      race: races,
      sex_at_birth: p.sex_at_birth,
      ethnicity: ethnicities
  }) AS participant_filters,
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
  CASE sm1.sample_id WHEN sm.sample_id THEN COLLECT(DISTINCT {
                              sample_anatomic_site: sm.anatomic_site,
                              participant_age_at_collection: sm.participant_age_at_collection,
                              sample_tumor_status: sm.sample_tumor_status,
                              tumor_classification: sm.tumor_classification
                          })
            ELSE apoc.coll.union(COLLECT(DISTINCT {
                              sample_anatomic_site: sm1.anatomic_site,
                              participant_age_at_collection: sm1.participant_age_at_collection,
                              sample_tumor_status: sm1.sample_tumor_status,
                              tumor_classification: sm1.tumor_classification
                          }), COLLECT(DISTINCT {
                              sample_anatomic_site: sm.anatomic_site,
                              participant_age_at_collection: sm.participant_age_at_collection,
                              sample_tumor_status: sm.sample_tumor_status,
                              tumor_classification: sm.tumor_classification
                          })) END AS sample_filters,
  COLLECT(DISTINCT stf.grant_id) as grant_id,
  COLLECT(DISTINCT stp.institution) as institution,
  null AS library_selection,
  null AS library_source,
  null AS library_strategy
UNION ALL
with st
MATCH (file)
WHERE (file:sequencing_file OR file:pathology_file OR file:methylation_array_file OR file:single_cell_sequencing_file OR file:cytogenomic_file)
MATCH (st)<-[:of_cell_line|of_pdx]-(cl)<--(sm:sample)
Where (cl: cell_line or cl: pdx)
MATCH (sm)<--(file)
OPTIONAL MATCH (st)<-[:of_publication]-(pub:publication)
OPTIONAL MATCH (st)<--(p:participant)<-[:of_diagnosis]-(dg:diagnosis)
OPTIONAL MATCH (st)<--(p)<-[:of_follow_up]-(fu:follow_up)
OPTIONAL MATCH (st)<-[:of_study_personnel]-(stp:study_personnel)
OPTIONAL MATCH (st)<-[:of_study_funding]-(stf:study_funding)
with file, sm, st, fu, dg, stf, stp
RETURN DISTINCT
  file.id as id,
  null as pid,
  CASE LABELS(file)[0]
        WHEN 'sequencing_file' THEN file.sequencing_file_id
        WHEN 'single_cell_sequencing_file' THEN file.single_cell_sequencing_file_id
        WHEN 'cytogenomic_file' THEN file.cytogenomic_file_id
        WHEN 'pathology_file' THEN file.pathology_file_id
        WHEN 'methylation_array_file' THEN file.methylation_array_file_id END AS file_id,
  file.dcf_indexd_guid AS guid,
  file.file_name AS file_name,
  CASE LABELS(file)[0]
        WHEN 'sequencing_file' THEN 'Sequencing'
        WHEN 'single_cell_sequencing_file' THEN 'Single Cell Sequencing'
        WHEN 'cytogenomic_file' THEN 'Cytogenomic'
        WHEN 'pathology_file' THEN 'Pathology imaging'
        WHEN 'methylation_array_file' THEN 'Methylation array' END AS file_category,
  file.file_type AS file_type,
  file.file_description AS file_description,
  file.file_size AS file_size,
  file.md5sum AS md5sum,
  st.study_id AS study_id,
  st.phs_accession as phs_accession,
  st.study_acronym as study_acronym,
  st.study_short_title as study_short_title,
  null AS participant_id,
  sm.sample_id AS sample_id,
  [null] AS participant_filters,
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
      sample_anatomic_site: sm.anatomic_site,
      participant_age_at_collection: sm.participant_age_at_collection,
      sample_tumor_status: sm.sample_tumor_status,
      tumor_classification: sm.tumor_classification
  }) AS sample_filters,
  COLLECT(DISTINCT stf.grant_id) as grant_id,
  COLLECT(DISTINCT stp.institution) as institution,
  CASE LABELS(file)[0]
            WHEN 'sequencing_file' THEN file.library_selection
            WHEN 'single_cell_sequencing_file' THEN file.library_selection
            ELSE null END AS library_selection,
  CASE LABELS(file)[0]
            WHEN 'sequencing_file' THEN file.library_source
            WHEN 'single_cell_sequencing_file' THEN file.library_source
            ELSE null END AS library_source,
  CASE LABELS(file)[0]
            WHEN 'sequencing_file' THEN file.library_strategy
            WHEN 'single_cell_sequencing_file' THEN file.library_strategy
            ELSE null END AS library_strategy
UNION ALL
with st
MATCH (st)<--(p:participant)<--(sm1:sample)<-[*2..2]-(sm:sample)
where not ((sm)<--(:sequencing_file)) and not ((sm)<--(:single_cell_sequencing_file)) and not ((sm)<--(:cytogenomic_file)) and not ((sm)<--(:pathology_file)) and not ((sm)<--(:methylation_array_file)) and not ((p)<--(:radiology_file)) and not ((p)<--(:clinical_measure_file))
OPTIONAL MATCH (p)<-[:of_diagnosis]-(dg:diagnosis)
OPTIONAL MATCH (p)<-[:of_follow_up]-(fu:follow_up)
OPTIONAL MATCH (st)<-[:of_study_personnel]-(stp:study_personnel)
OPTIONAL MATCH (st)<-[:of_study_funding]-(stf:study_funding)
UNWIND apoc.text.split(p.ethnicity, ';') AS ethnicities
UNWIND apoc.text.split(p.race, ';') AS races
with sm1, sm, p, ethnicities, races, st, dg, fu, stf, stp
RETURN DISTINCT
    null as id,
    p.id as pid,
    null AS file_id,
    null AS guid,
    null AS file_name,
    null AS file_category,
    null AS file_type,
    null AS file_description,
    null AS file_size,
    null AS md5sum,
    st.study_id AS study_id,
    st.phs_accession as phs_accession,
    st.study_acronym as study_acronym,
    st.study_short_title as study_short_title,
    p.participant_id AS participant_id,
    sm.sample_id AS sample_id,
    COLLECT(DISTINCT {
        race: races,
        sex_at_birth: p.sex_at_birth,
        ethnicity: ethnicities
    }) AS participant_filters,
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
    apoc.coll.union(COLLECT(DISTINCT {
                              sample_anatomic_site: sm1.anatomic_site,
                              participant_age_at_collection: sm1.participant_age_at_collection,
                              sample_tumor_status: sm1.sample_tumor_status,
                              tumor_classification: sm1.tumor_classification
                          }), COLLECT(DISTINCT {
                              sample_anatomic_site: sm.anatomic_site,
                              participant_age_at_collection: sm.participant_age_at_collection,
                              sample_tumor_status: sm.sample_tumor_status,
                              tumor_classification: sm.tumor_classification
                          })) AS sample_filters,
    COLLECT(DISTINCT stf.grant_id) as grant_id,
    COLLECT(DISTINCT stp.institution) as institution,
    null AS library_selection,
    null AS library_source,
    null AS library_strategy
UNION ALL
with st
MATCH (st)<--(p:participant)<--(sm:sample)
where not ((sm)<-[*..3]-(:sequencing_file)) and not ((sm)<-[*..3]-(:single_cell_sequencing_file)) and not ((sm)<-[*..3]-(:cytogenomic_file)) and not ((sm)<-[*..3]-(:pathology_file)) and not ((sm)<-[*..3]-(:methylation_array_file)) and not ((p)<--(:radiology_file)) and not ((p)<--(:clinical_measure_file))
OPTIONAL MATCH (p)<-[:of_diagnosis]-(dg:diagnosis)
OPTIONAL MATCH (p)<-[:of_follow_up]-(fu:follow_up)
OPTIONAL MATCH (st)<-[:of_study_personnel]-(stp:study_personnel)
OPTIONAL MATCH (st)<-[:of_study_funding]-(stf:study_funding)
UNWIND apoc.text.split(p.ethnicity, ';') AS ethnicities
UNWIND apoc.text.split(p.race, ';') AS races
with sm, p, ethnicities, races, st, dg, fu, stf, stp
RETURN DISTINCT
    null as id,
    p.id as pid,
    null AS file_id,
    null AS guid,
    null AS file_name,
    null AS file_category,
    null AS file_type,
    null AS file_description,
    null AS file_size,
    null AS md5sum,
    st.study_id AS study_id,
    st.phs_accession as phs_accession,
    st.study_acronym as study_acronym,
    st.study_short_title as study_short_title,
    p.participant_id AS participant_id,
    sm.sample_id AS sample_id,
    COLLECT(DISTINCT {
        race: races,
        sex_at_birth: p.sex_at_birth,
        ethnicity: ethnicities
    }) AS participant_filters,
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
        sample_anatomic_site: sm.anatomic_site,
        participant_age_at_collection: sm.participant_age_at_collection,
        sample_tumor_status: sm.sample_tumor_status,
        tumor_classification: sm.tumor_classification
    }) AS sample_filters,
    COLLECT(DISTINCT stf.grant_id) as grant_id,
    COLLECT(DISTINCT stp.institution) as institution,
    null AS library_selection,
    null AS library_source,
    null AS library_strategy
UNION ALL
with st
MATCH (st)<--(cl)<--(sm:sample)
Where (cl: cell_line or cl: pdx) and not ((sm)<--(:sequencing_file)) and not ((sm)<--(:single_cell_sequencing_file)) and not ((sm)<--(:cytogenomic_file)) and not ((sm)<--(:pathology_file)) and not ((sm)<--(:methylation_array_file))
OPTIONAL MATCH (st)<-[:of_study_personnel]-(stp:study_personnel)
OPTIONAL MATCH (st)<-[:of_study_funding]-(stf:study_funding)
with sm, st, stf, stp
RETURN DISTINCT
    null as id,
    null as pid,
    null AS file_id,
    null AS guid,
    null AS file_name,
    null AS file_category,
    null AS file_type,
    null AS file_description,
    null AS file_size,
    null AS md5sum,
    st.study_id AS study_id,
    st.phs_accession as phs_accession,
    st.study_acronym as study_acronym,
    st.study_short_title as study_short_title,
    null AS participant_id,
    sm.sample_id AS sample_id,
    [null] AS participant_filters,
    [null] AS diagnosis_filters,
    [null] as vital_status,
    COLLECT(DISTINCT {
        sample_anatomic_site: sm.anatomic_site,
        participant_age_at_collection: sm.participant_age_at_collection,
        sample_tumor_status: sm.sample_tumor_status,
        tumor_classification: sm.tumor_classification
    }) AS sample_filters,
    COLLECT(DISTINCT stf.grant_id) as grant_id,
    COLLECT(DISTINCT stp.institution) as institution,
    null AS library_selection,
    null AS library_source,
    null AS library_strategy
UNION ALL
with st
MATCH (st)<--(p:participant)
where not ((p)<--(:sample)) and not ((p)<--(:radiology_file)) and not ((p)<--(:clinical_measure_file))
OPTIONAL MATCH (p)<-[:of_diagnosis]-(dg:diagnosis)
OPTIONAL MATCH (p)<-[:of_follow_up]-(fu:follow_up)
OPTIONAL MATCH (st)<-[:of_study_personnel]-(stp:study_personnel)
OPTIONAL MATCH (st)<-[:of_study_funding]-(stf:study_funding)
UNWIND apoc.text.split(p.ethnicity, ';') AS ethnicities
UNWIND apoc.text.split(p.race, ';') AS races
with p, ethnicities, races, st, dg, fu, stf, stp
RETURN DISTINCT
    null as id,
    p.id as pid,
    null AS file_id,
    null AS guid,
    null AS file_name,
    null AS file_category,
    null AS file_type,
    null AS file_description,
    null AS file_size,
    null AS md5sum,
    st.study_id AS study_id,
    st.phs_accession as phs_accession,
    st.study_acronym as study_acronym,
    st.study_short_title as study_short_title,
    p.participant_id AS participant_id,
    null AS sample_id,
    COLLECT(DISTINCT {
        race: races,
        sex_at_birth: p.sex_at_birth,
        ethnicity: ethnicities
    }) AS participant_filters,
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
    [null] AS sample_filters,
    COLLECT(DISTINCT stf.grant_id) as grant_id,
    COLLECT(DISTINCT stp.institution) as institution,
    null AS library_selection,
    null AS library_source,
    null AS library_strategy
}
with id, guid, file_name, file_category, file_type, file_description, file_size, md5sum, study_id, phs_accession, study_acronym, study_short_title, participant_id, sample_id, participant_filters,diagnosis_filters, vital_status, sample_filters, grant_id, institution,library_selection,library_source,library_strategy
unwind participant_filters as participant_filter
with id, guid, file_name, file_category, file_type, file_description, file_size, md5sum, study_id, phs_accession, study_acronym, study_short_title, participant_id, sample_id, participant_filter,diagnosis_filters, vital_status, sample_filters, grant_id, institution,library_selection,library_source,library_strategy
where participant_id in [''] and participant_filter.sex_at_birth in [''] and ANY(element IN [''] WHERE element IN participant_filter.race) and ANY(element IN [''] WHERE element IN participant_filter.ethnicity)
unwind diagnosis_filters as diagnosis_filter
with id, guid, file_name, file_category, file_type, file_description, file_size, md5sum, study_id, phs_accession, study_acronym, study_short_title, participant_id, sample_id,diagnosis_filter, vital_status, sample_filters, grant_id, institution,library_selection,library_source,library_strategy
where diagnosis_filter.age_at_diagnosis >= [''] and diagnosis_filter.age_at_diagnosis <= [''] and diagnosis_filter.diagnosis_anatomic_site in [''] and diagnosis_filter.diagnosis_classification in [''] and diagnosis_filter.diagnosis_classification_system in [''] and diagnosis_filter.diagnosis_verification_status in [''] and diagnosis_filter.diagnosis_basis in [''] and diagnosis_filter.disease_phase in ['']
with id, guid, file_name, file_category, file_type, file_description, file_size, md5sum, study_id, phs_accession, study_acronym, study_short_title, participant_id, sample_id, vital_status, sample_filters, grant_id, institution,library_selection,library_source,library_strategy
where ANY(element IN [''] WHERE element IN vital_status)
unwind sample_filters as sample_filter
with id, guid, file_name, file_category, file_type, file_description, file_size, md5sum, study_id, phs_accession, study_acronym, study_short_title, participant_id, sample_id, sample_filter, grant_id, institution,library_selection,library_source,library_strategy
where sample_filter.participant_age_at_collection >= [''] and sample_filter.participant_age_at_collection <= [''] and sample_filter.sample_anatomic_site in [''] and sample_filter.sample_tumor_status in [''] and sample_filter.tumor_classification in ['']
with distinct id, guid, file_name, file_category, file_type, file_description, file_size, md5sum, study_id, phs_accession, study_acronym, study_short_title, participant_id, sample_id, grant_id, institution,library_selection,library_source,library_strategy
call {
  with id, guid, file_name, file_category, file_type, file_description, file_size, md5sum, study_id, phs_accession, study_acronym, study_short_title, participant_id, sample_id, grant_id, institution,library_selection,library_source,library_strategy
  return id as fid, guid as dig, file_name as fn, file_category as fc, file_type as ft, file_description as fd, file_size as fsize, md5sum as md5, study_id as sid, phs_accession as pa, study_acronym as sa, study_short_title as sst, participant_id as pid, sample_id as smid, grant_id as gid, institution as istt,library_selection as ls,library_source as lis,library_strategy as listr
  UNION ALL
  with study_id
  MATCH (file:clinical_measure_file)
  MATCH (stu:study)<-[:of_clinical_measure_file]-(file)
  where stu.study_id = study_id
  OPTIONAL MATCH (stu)<-[:of_study_personnel]-(stp:study_personnel)
  OPTIONAL MATCH (stu)<-[:of_study_funding]-(stf:study_funding)
  With file, stu, stf, stp
  RETURN DISTINCT
    file.id as fid,
    file.dcf_indexd_guid AS dig,
    file.file_name AS fn,
    'Clinical data' AS fc,
    file.file_type AS ft,
    file.file_description AS fd,
    file.file_size AS fsize,
    file.md5sum AS md5,
    stu.study_id AS sid,
    stu.phs_accession as pa,
    stu.study_acronym as sa,
    stu.study_short_title as sst,
    null AS pid,
    null AS smid,
    COLLECT(DISTINCT stf.grant_id) as gid,
    COLLECT(DISTINCT stp.institution) as istt,
    null AS ls,
    null AS lis,
    null AS listr
}
with fid as id, dig as guid, fn as file_name, fc as file_category, ft as file_type, fd as file_description, fsize as file_size, md5 as md5sum, sid as study_id, pa as phs_accession, sa as study_acronym, sst as study_short_title, pid as participant_id, smid as sample_id, gid as grant_id, istt as institution,ls as library_selection,lis as library_source,listr as library_strategy
where file_category in [''] and file_type in [''] and ANY(element IN [''] WHERE element IN grant_id) and ANY(element IN [''] WHERE element IN institution) and study_acronym in [''] and study_short_title in [''] and library_selection in [''] and library_source in [''] and library_strategy in ['']
with id, guid, file_name, file_category, file_type, file_description, file_size, ['Bytes', 'KB', 'MB', 'GB', 'TB'] AS units,
        toInteger(floor(log(file_size)/log(1024))) as i,
        2 as precision, md5sum, study_id, phs_accession, study_acronym, study_short_title, participant_id, sample_id, grant_id, institution,library_selection,library_source,library_strategy
with id, guid, file_name, file_category, file_type, file_description, file_size, file_size /(1024^i) AS value,
        10^precision AS factor,
        units[i] as unit, md5sum, study_id, phs_accession, study_acronym, study_short_title, participant_id, sample_id, grant_id, institution,library_selection,library_source,library_strategy
with id, guid, file_name, file_category, file_type, file_description, unit,
        round(factor * value)/factor AS size, md5sum, study_id, phs_accession, study_acronym, study_short_title, participant_id, sample_id, grant_id, institution,library_selection,library_source,library_strategy     
with DISTINCT id,
        file_name,
        file_category,
        file_description,
        file_type,
        CASE size % 1 WHEN 0 THEN apoc.convert.toInteger(size)+' ' +unit ELSE size+' ' +unit END AS file_size_new,
        study_id,
        participant_id,
        sample_id,
        guid,
        md5sum,
        library_selection,
        library_source,
        library_strategy
where id IS NOT NULL
RETURN file_name AS `File Name`,
file_category As `File Category`,
file_description As `File Description`,
file_type As `File Type`,
file_size_new As `File Size`,
study_id As `Study ID`,
coalesce(participant_id, '') As `Participant ID`,
coalesce(sample_id, '') As `Sample ID`,
guid As `GUID`,
md5sum As `MD5Sum`,
coalesce(library_selection, '') As `Library Selection`,
coalesce(library_source, '') As `Library Source`,
coalesce(library_strategy, '') As `Library Strategy` 
ORDER BY file_name LIMIT 100
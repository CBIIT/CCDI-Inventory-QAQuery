Match (st:study)
where st.dbgap_accession in [''] and st.study_status in ['']
with st
Call {
    with st
    MATCH (file)
    where (file:clinical_measure_file or file: generic_file or file:radiology_file)
    MATCH (st)<--(p:participant)<--(file)
    optional MATCH (p)<-[*0..3]-(sm:sample)
    OPTIONAL MATCH (p)<-[:of_diagnosis]-(dg:diagnosis)
    with file, COLLECT(DISTINCT {
                                sample_anatomic_site: apoc.text.split(sm.anatomic_site, ';'),
                                participant_age_at_collection: sm.participant_age_at_collection,
                                sample_tumor_status: sm.sample_tumor_status,
                                tumor_classification: sm.tumor_classification,
                                age_at_diagnosis: dg.age_at_diagnosis,
                                diagnosis_anatomic_site: apoc.text.split(dg.anatomic_site, ';'),
                                disease_phase: dg.disease_phase,
                                diagnosis_classification_system: dg.diagnosis_classification_system,
                                diagnosis_basis: dg.diagnosis_basis,
                                tumor_grade_source: dg.tumor_grade_source,
                                tumor_stage_source: dg.tumor_stage_source,
                                diagnosis: dg.diagnosis
                            }) AS sample_diagnosis_filter_1
    MATCH (p:participant)<--(file)
    optional MATCH (p)<-[:of_sample]-(sm:sample)
    OPTIONAL MATCH (sm)<--(dg:diagnosis)
    with file, sample_diagnosis_filter_1, COLLECT(DISTINCT {
                                sample_anatomic_site: apoc.text.split(sm.anatomic_site, ';'),
                                participant_age_at_collection: sm.participant_age_at_collection,
                                sample_tumor_status: sm.sample_tumor_status,
                                tumor_classification: sm.tumor_classification,
                                age_at_diagnosis: dg.age_at_diagnosis,
                                diagnosis_anatomic_site: apoc.text.split(dg.anatomic_site, ';'),
                                disease_phase: dg.disease_phase,
                                diagnosis_classification_system: dg.diagnosis_classification_system,
                                diagnosis_basis: dg.diagnosis_basis,
                                tumor_grade_source: dg.tumor_grade_source,
                                tumor_stage_source: dg.tumor_stage_source,
                                diagnosis: dg.diagnosis
                            }) AS sample_diagnosis_filter_2
    with file, apoc.coll.union(sample_diagnosis_filter_1, sample_diagnosis_filter_2) as sample_diagnosis_filter_3
    MATCH (p:participant)<--(file)
    optional MATCH (p)<-[:of_sample]-(sm1:sample)<-[*2..2]-(sm:sample)
    OPTIONAL MATCH (sm1)<--(dg:diagnosis)
    with file, sample_diagnosis_filter_3, COLLECT(DISTINCT {
                                sample_anatomic_site: apoc.text.split(sm.anatomic_site, ';'),
                                participant_age_at_collection: sm.participant_age_at_collection,
                                sample_tumor_status: sm.sample_tumor_status,
                                tumor_classification: sm.tumor_classification,
                                age_at_diagnosis: dg.age_at_diagnosis,
                                diagnosis_anatomic_site: apoc.text.split(dg.anatomic_site, ';'),
                                disease_phase: dg.disease_phase,
                                diagnosis_classification_system: dg.diagnosis_classification_system,
                                diagnosis_basis: dg.diagnosis_basis,
                                tumor_grade_source: dg.tumor_grade_source,
                                tumor_stage_source: dg.tumor_stage_source,
                                diagnosis: dg.diagnosis
                            }) AS sample_diagnosis_filter_4
    with file, apoc.coll.union(sample_diagnosis_filter_3, sample_diagnosis_filter_4) as sample_diagnosis_filter_5
    MATCH (p:participant)<--(file)
    optional MATCH (p)<-[:of_sample]-(sm1:sample)<-[*2..2]-(sm:sample)
    OPTIONAL MATCH (sm)<--(dg:diagnosis)
    with file, sample_diagnosis_filter_5, apoc.coll.union(COLLECT(DISTINCT {
                                sample_anatomic_site: apoc.text.split(sm1.anatomic_site, ';'),
                                participant_age_at_collection: sm1.participant_age_at_collection,
                                sample_tumor_status: sm1.sample_tumor_status,
                                tumor_classification: sm1.tumor_classification,
                                age_at_diagnosis: dg.age_at_diagnosis,
                                diagnosis_anatomic_site: apoc.text.split(dg.anatomic_site, ';'),
                                disease_phase: dg.disease_phase,
                                diagnosis_classification_system: dg.diagnosis_classification_system,
                                diagnosis_basis: dg.diagnosis_basis,
                                tumor_grade_source: dg.tumor_grade_source,
                                tumor_stage_source: dg.tumor_stage_source,
                                diagnosis: dg.diagnosis
                            }), COLLECT(DISTINCT {
                                sample_anatomic_site: apoc.text.split(sm.anatomic_site, ';'),
                                participant_age_at_collection: sm.participant_age_at_collection,
                                sample_tumor_status: sm.sample_tumor_status,
                                tumor_classification: sm.tumor_classification,
                                age_at_diagnosis: dg.age_at_diagnosis,
                                diagnosis_anatomic_site: apoc.text.split(dg.anatomic_site, ';'),
                                disease_phase: dg.disease_phase,
                                diagnosis_classification_system: dg.diagnosis_classification_system,
                                diagnosis_basis: dg.diagnosis_basis,
                                tumor_grade_source: dg.tumor_grade_source,
                                tumor_stage_source: dg.tumor_stage_source,
                                diagnosis: dg.diagnosis
                            })) AS sample_diagnosis_filter_6
    with file, apoc.coll.union(sample_diagnosis_filter_5, sample_diagnosis_filter_6) as sample_diagnosis_filter
    MATCH (p:participant)<--(file)
    MATCH (st:study)<-[:of_participant]-(p)
    OPTIONAL MATCH (st)<-[:of_publication]-(pub:publication)
    OPTIONAL MATCH (p)<-[:of_survival]-(su:survival)
    OPTIONAL MATCH (p)<-[:of_treatment]-(tm:treatment)
    OPTIONAL MATCH (p)<-[:of_treatment_response]-(tr:treatment_response)
    OPTIONAL MATCH (st)<-[:of_study_personnel]-(stp:study_personnel)
    OPTIONAL MATCH (st)<-[:of_study_funding]-(stf:study_funding)
    with file, p, st, sample_diagnosis_filter,COLLECT(DISTINCT {last_known_survival_status: su.last_known_survival_status, 
        event_free_survival_status: su.event_free_survival_status, 
        first_event: su.first_event,
        age_at_last_known_survival_status: su.age_at_last_known_survival_status} ) AS survival_filters,
    COLLECT(DISTINCT{treatment_type: apoc.text.split(tm.treatment_type, ';'),
    treatment_agent: apoc.text.split(tm.treatment_agent, ';'),
    age_at_treatment_start: tm.age_at_treatment_start}) as treatment_filters,
    COLLECT(DISTINCT{response_category: tr.response_category,
    age_at_response: tr.age_at_response}) as treatment_response_filters
    RETURN DISTINCT
    file.id as id,
    p.id as pid,
    CASE LABELS(file)[0]
            WHEN 'clinical_measure_file' THEN file.clinical_measure_file_id
            WHEN 'generic_file' THEN file.generic_file_id
            WHEN 'radiology_file' THEN file.radiology_file_id
            ELSE null END AS file_id,
    file.dcf_indexd_guid AS guid,
    file.file_name AS file_name,
    file.file_access AS file_access,
    apoc.text.split(file.data_category, ';') As data_category,
    file.file_type AS file_type,
    file.file_mapping_level as file_mapping_level,
    file.file_description AS file_description,
    file.file_size AS file_size,
    file.md5sum AS md5sum,
    st.study_id AS study_id,
    st.dbgap_accession as dbgap_accession,
    st.study_acronym as study_acronym,
    st.study_name as study_name,
    p.participant_id AS participant_id,
    null AS sample_id,
    COLLECT(DISTINCT {
        race: apoc.text.split(p.race, ';'),
        sex_at_birth: p.sex_at_birth
    }) AS participant_filters,
    sample_diagnosis_filter AS sample_diagnosis_filters,
    survival_filters as survival_filters,  
    treatment_filters as treatment_filters,
    treatment_response_filters as treatment_response_filters,
    null AS library_selection,
    null AS library_source_material,
    null AS library_source_molecule,
    null AS library_strategy
    union all
    with st
    MATCH (file)
    where (file: sequencing_file or file: generic_file OR file:pathology_file OR file:methylation_array_file OR file:cytogenomic_file)
    MATCH (st)<--(p:participant)<-[*..3]-(sm:sample)<--(file)
    where p.participant_id in [''] and p.sex_at_birth in [''] and ANY(element IN [''] WHERE element IN apoc.text.split(p.race, ';')) 
            and sm.participant_age_at_collection >= [''] and sm.participant_age_at_collection <= [''] and ANY(element IN [''] WHERE element IN apoc.text.split(sm.anatomic_site, ';')) and sm.sample_tumor_status in [''] and sm.tumor_classification in ['']
    OPTIONAL MATCH (p)<-[:of_diagnosis]-(dg:diagnosis)
    where dg.age_at_diagnosis >= [''] and dg.age_at_diagnosis <= [''] and dg.diagnosis in [''] and ANY(element IN [''] WHERE element IN apoc.text.split(dg.anatomic_site, ';')) and dg.diagnosis_classification_system in [''] and dg.diagnosis_basis in [''] and dg.disease_phase in ['']
    with file, COLLECT(DISTINCT {
                                sample_anatomic_site: apoc.text.split(sm.anatomic_site, ';'),
                                participant_age_at_collection: sm.participant_age_at_collection,
                                sample_tumor_status: sm.sample_tumor_status,
                                tumor_classification: sm.tumor_classification,
                                age_at_diagnosis: dg.age_at_diagnosis,
                                diagnosis_anatomic_site: apoc.text.split(dg.anatomic_site, ';'),
                                disease_phase: dg.disease_phase,
                                diagnosis_classification_system: dg.diagnosis_classification_system,
                                diagnosis_basis: dg.diagnosis_basis,
                                tumor_grade_source: dg.tumor_grade_source,
                                tumor_stage_source: dg.tumor_stage_source,
                                diagnosis: dg.diagnosis
                            }) AS sample_diagnosis_filter_1
    optional MATCH (p:participant)<-[:of_sample]-(sm:sample)<--(file)
    OPTIONAL MATCH (sm)<--(dg:diagnosis)
    with file, sample_diagnosis_filter_1, COLLECT(DISTINCT {
                                sample_anatomic_site: apoc.text.split(sm.anatomic_site, ';'),
                                participant_age_at_collection: sm.participant_age_at_collection,
                                sample_tumor_status: sm.sample_tumor_status,
                                tumor_classification: sm.tumor_classification,
                                age_at_diagnosis: dg.age_at_diagnosis,
                                diagnosis_anatomic_site: apoc.text.split(dg.anatomic_site, ';'),
                                disease_phase: dg.disease_phase,
                                diagnosis_classification_system: dg.diagnosis_classification_system,
                                diagnosis_basis: dg.diagnosis_basis,
                                tumor_grade_source: dg.tumor_grade_source,
                                tumor_stage_source: dg.tumor_stage_source,
                                diagnosis: dg.diagnosis
                            }) AS sample_diagnosis_filter_2
    with file, apoc.coll.union(sample_diagnosis_filter_1, sample_diagnosis_filter_2) as sample_diagnosis_filter_3
    optional MATCH (p:participant)<-[:of_sample]-(sm1:sample)<-[*2..2]-(sm:sample)<--(file)
    OPTIONAL MATCH (sm1)<--(dg:diagnosis)
    with file, sample_diagnosis_filter_3, apoc.coll.union(COLLECT(DISTINCT {
                                sample_anatomic_site: apoc.text.split(sm1.anatomic_site, ';'),
                                participant_age_at_collection: sm1.participant_age_at_collection,
                                sample_tumor_status: sm1.sample_tumor_status,
                                tumor_classification: sm1.tumor_classification,
                                age_at_diagnosis: dg.age_at_diagnosis,
                                diagnosis_anatomic_site: apoc.text.split(dg.anatomic_site, ';'),
                                disease_phase: dg.disease_phase,
                                diagnosis_classification_system: dg.diagnosis_classification_system,
                                diagnosis_basis: dg.diagnosis_basis,
                                tumor_grade_source: dg.tumor_grade_source,
                                tumor_stage_source: dg.tumor_stage_source,
                                diagnosis: dg.diagnosis
                            }), COLLECT(DISTINCT {
                                sample_anatomic_site: apoc.text.split(sm.anatomic_site, ';'),
                                participant_age_at_collection: sm.participant_age_at_collection,
                                sample_tumor_status: sm.sample_tumor_status,
                                tumor_classification: sm.tumor_classification,
                                age_at_diagnosis: dg.age_at_diagnosis,
                                diagnosis_anatomic_site: apoc.text.split(dg.anatomic_site, ';'),
                                disease_phase: dg.disease_phase,
                                diagnosis_classification_system: dg.diagnosis_classification_system,
                                diagnosis_basis: dg.diagnosis_basis,
                                tumor_grade_source: dg.tumor_grade_source,
                                tumor_stage_source: dg.tumor_stage_source,
                                diagnosis: dg.diagnosis
                            })) AS sample_diagnosis_filter_4
    with file, apoc.coll.union(sample_diagnosis_filter_3, sample_diagnosis_filter_4) as sample_diagnosis_filter_5
    optional MATCH (p:participant)<-[:of_sample]-(sm1:sample)<-[*2..2]-(sm:sample)<--(file)
    OPTIONAL MATCH (sm)<--(dg:diagnosis)
    with file, sample_diagnosis_filter_5, apoc.coll.union(COLLECT(DISTINCT {
                                sample_anatomic_site: apoc.text.split(sm1.anatomic_site, ';'),
                                participant_age_at_collection: sm1.participant_age_at_collection,
                                sample_tumor_status: sm1.sample_tumor_status,
                                tumor_classification: sm1.tumor_classification,
                                age_at_diagnosis: dg.age_at_diagnosis,
                                diagnosis_anatomic_site: apoc.text.split(dg.anatomic_site, ';'),
                                disease_phase: dg.disease_phase,
                                diagnosis_classification_system: dg.diagnosis_classification_system,
                                diagnosis_basis: dg.diagnosis_basis,
                                tumor_grade_source: dg.tumor_grade_source,
                                tumor_stage_source: dg.tumor_stage_source,
                                diagnosis: dg.diagnosis
                            }), COLLECT(DISTINCT {
                                sample_anatomic_site: apoc.text.split(sm.anatomic_site, ';'),
                                participant_age_at_collection: sm.participant_age_at_collection,
                                sample_tumor_status: sm.sample_tumor_status,
                                tumor_classification: sm.tumor_classification,
                                age_at_diagnosis: dg.age_at_diagnosis,
                                diagnosis_anatomic_site: apoc.text.split(dg.anatomic_site, ';'),
                                disease_phase: dg.disease_phase,
                                diagnosis_classification_system: dg.diagnosis_classification_system,
                                diagnosis_basis: dg.diagnosis_basis,
                                tumor_grade_source: dg.tumor_grade_source,
                                tumor_stage_source: dg.tumor_stage_source,
                                diagnosis: dg.diagnosis
                            })) AS sample_diagnosis_filter_6
    with file, apoc.coll.union(sample_diagnosis_filter_5, sample_diagnosis_filter_6) as sample_diagnosis_filter
    MATCH (p:participant)<-[:of_sample]-(sm1:sample)<-[*0..2]-(sm:sample)<--(file)
    MATCH (st:study)<-[:of_participant]-(p)
    OPTIONAL MATCH (st)<-[:of_publication]-(pub:publication)
    OPTIONAL MATCH (p)<-[:of_survival]-(su:survival)
    OPTIONAL MATCH (p)<-[:of_treatment]-(tm:treatment)
    OPTIONAL MATCH (p)<-[:of_treatment_response]-(tr:treatment_response)
    OPTIONAL MATCH (st)<-[:of_study_personnel]-(stp:study_personnel)
    OPTIONAL MATCH (st)<-[:of_study_funding]-(stf:study_funding)
    with file, p, sample_diagnosis_filter, sm1, sm, st, COLLECT(DISTINCT {last_known_survival_status: su.last_known_survival_status, 
              event_free_survival_status: su.event_free_survival_status, 
              first_event: su.first_event,
              age_at_last_known_survival_status: su.age_at_last_known_survival_status} ) AS survival_filters,
            COLLECT(DISTINCT{treatment_type: apoc.text.split(tm.treatment_type, ';'),
            treatment_agent: apoc.text.split(tm.treatment_agent, ';'),
            age_at_treatment_start: tm.age_at_treatment_start}) as treatment_filters,
            COLLECT(DISTINCT{response_category: tr.response_category,
            age_at_response: tr.age_at_response}) as treatment_response_filters, stf, stp
    RETURN DISTINCT
    file.id as id,
    p.id as pid,
    CASE LABELS(file)[0]
            WHEN 'sequencing_file' THEN file.sequencing_file_id
            WHEN 'generic_file' THEN file.generic_file_id
            WHEN 'cytogenomic_file' THEN file.cytogenomic_file_id
            WHEN 'pathology_file' THEN file.pathology_file_id
            WHEN 'methylation_array_file' THEN file.methylation_array_file_id ELSE null END AS file_id,
    file.dcf_indexd_guid AS guid,
    file.file_name AS file_name,
    file.file_access AS file_access,
    apoc.text.split(file.data_category, ';') As data_category,
    file.file_type AS file_type,
    file.file_mapping_level AS file_mapping_level,
    file.file_description AS file_description,
    file.file_size AS file_size,
    file.md5sum AS md5sum,
    st.study_id AS study_id,
    st.dbgap_accession as dbgap_accession,
    st.study_acronym as study_acronym,
    st.study_name as study_name,
    p.participant_id AS participant_id,
    CASE sm1.sample_id WHEN sm.sample_id THEN sm.sample_id
                ELSE sm1.sample_id + ',' + sm.sample_id END AS sample_id,
    COLLECT(DISTINCT {
        race: apoc.text.split(p.race, ';'),
        sex_at_birth: p.sex_at_birth
    }) AS participant_filters,
    sample_diagnosis_filter AS sample_diagnosis_filters,
    survival_filters as survival_filters,
    treatment_filters as treatment_filters,
    treatment_response_filters as treatment_response_filters,
    CASE LABELS(file)[0] WHEN 'sequencing_file' THEN file.library_selection
                                ELSE null END AS library_selection,
    CASE LABELS(file)[0] WHEN 'sequencing_file' THEN file.library_source_material
                                ELSE null END AS library_source_material,
    CASE LABELS(file)[0] WHEN 'sequencing_file' THEN file.library_source_molecule
                                ELSE null END AS library_source_molecule,
    CASE LABELS(file)[0] WHEN 'sequencing_file' THEN file.library_strategy
                                ELSE null END AS library_strategy
    union all
    with st
    MATCH (file)
    WHERE (file:sequencing_file or file:generic_file OR file:pathology_file OR file:methylation_array_file OR file:cytogenomic_file)
    MATCH (st)<-[:of_cell_line|of_pdx]-(cl)<--(sm:sample)
    Where (cl: cell_line or cl: pdx)
    MATCH (sm)<--(file)
    optional match (sm)<--(dg:diagnosis)
    OPTIONAL MATCH (st)<-[:of_publication]-(pub:publication)
    OPTIONAL MATCH (st)<-[:of_study_personnel]-(stp:study_personnel)
    OPTIONAL MATCH (st)<-[:of_study_funding]-(stf:study_funding)
    with file, sm, st, dg, stf, stp
    RETURN DISTINCT
    file.id as id,
    null as pid,
    CASE LABELS(file)[0]
            WHEN 'sequencing_file' THEN file.sequencing_file_id
            WHEN 'generic_file' THEN file.generic_file_id
            WHEN 'cytogenomic_file' THEN file.cytogenomic_file_id
            WHEN 'pathology_file' THEN file.pathology_file_id
            WHEN 'methylation_array_file' THEN file.methylation_array_file_id ELSE null END AS file_id,
    file.dcf_indexd_guid AS guid,
    file.file_name AS file_name,
    file.file_access AS file_access,
    apoc.text.split(file.data_category, ';') As data_category,
    file.file_type AS file_type,
    file.file_mapping_level AS file_mapping_level,
    file.file_description AS file_description,
    file.file_size AS file_size,
    file.md5sum AS md5sum,
    st.study_id AS study_id,
    st.dbgap_accession as dbgap_accession,
    st.study_acronym as study_acronym,
    st.study_name as study_name,
    null AS participant_id,
    sm.sample_id AS sample_id,
    null AS participant_filters,
    COLLECT(DISTINCT {
        sample_anatomic_site: apoc.text.split(sm.anatomic_site, ';'),
        participant_age_at_collection: sm.participant_age_at_collection,
        sample_tumor_status: sm.sample_tumor_status,
        tumor_classification: sm.tumor_classification,
        age_at_diagnosis: dg.age_at_diagnosis,
        diagnosis_anatomic_site: apoc.text.split(dg.anatomic_site, ';'),
        disease_phase: dg.disease_phase,
        diagnosis_classification_system: dg.diagnosis_classification_system,
        diagnosis_basis: dg.diagnosis_basis,
        tumor_grade_source: dg.tumor_grade_source,
        tumor_stage_source: dg.tumor_stage_source,
        diagnosis: dg.diagnosis
    }) AS sample_diagnosis_filters, 
    null as survival_filters,
    null as treatment_filters,
    null as treatment_response_filters,
    CASE LABELS(file)[0]
                WHEN 'sequencing_file' THEN file.library_selection
                ELSE null END AS library_selection,
    CASE LABELS(file)[0] WHEN 'sequencing_file' THEN file.library_source_material
                                ELSE null END AS library_source_material,
    CASE LABELS(file)[0] WHEN 'sequencing_file' THEN file.library_source_molecule
                                ELSE null END AS library_source_molecule,
    CASE LABELS(file)[0]
                WHEN 'sequencing_file' THEN file.library_strategy
                ELSE null END AS library_strategy
    union all
    with st
    MATCH (st)<--(p:participant)<--(sm1:sample)<-[*2..2]-(sm:sample)
    where not ((sm)<--(:sequencing_file)) and not ((sm)<--(:generic_file)) and not ((sm)<--(:cytogenomic_file)) and not ((sm)<--(:pathology_file)) and not ((sm)<--(:methylation_array_file)) and not ((p)<--(:radiology_file)) and not ((p)<--(:clinical_measure_file)) and not ((p)<--(:generic_file))
    OPTIONAL MATCH (p)<-[:of_diagnosis]-(dg:diagnosis)
    with p, sm1, sm, apoc.coll.union(COLLECT(DISTINCT {
                        sample_anatomic_site: apoc.text.split(sm1.anatomic_site, ';'),
                        participant_age_at_collection: sm1.participant_age_at_collection,
                        sample_tumor_status: sm1.sample_tumor_status,
                        tumor_classification: sm1.tumor_classification,
                        age_at_diagnosis: dg.age_at_diagnosis,
                        diagnosis_anatomic_site: apoc.text.split(dg.anatomic_site, ';'),
                        disease_phase: dg.disease_phase,
                        diagnosis_classification_system: dg.diagnosis_classification_system,
                        diagnosis_basis: dg.diagnosis_basis,
                        tumor_grade_source: dg.tumor_grade_source,
                        tumor_stage_source: dg.tumor_stage_source,
                        diagnosis: dg.diagnosis
                    }), COLLECT(DISTINCT {
                        sample_anatomic_site: apoc.text.split(sm.anatomic_site, ';'),
                        participant_age_at_collection: sm.participant_age_at_collection,
                        sample_tumor_status: sm.sample_tumor_status,
                        tumor_classification: sm.tumor_classification,
                        age_at_diagnosis: dg.age_at_diagnosis,
                        diagnosis_anatomic_site: apoc.text.split(dg.anatomic_site, ';'),
                        disease_phase: dg.disease_phase,
                        diagnosis_classification_system: dg.diagnosis_classification_system,
                        diagnosis_basis: dg.diagnosis_basis,
                        tumor_grade_source: dg.tumor_grade_source,
                        tumor_stage_source: dg.tumor_stage_source,
                        diagnosis: dg.diagnosis
                    })) as sample_diagnosis_filter_1
    MATCH (st:study)<--(p)<--(sm1)<-[*2..2]-(sm)
    where not ((sm)<--(:sequencing_file)) and not ((sm)<--(:generic_file)) and not ((sm)<--(:cytogenomic_file)) and not ((sm)<--(:pathology_file)) and not ((sm)<--(:methylation_array_file)) and not ((p)<--(:radiology_file)) and not ((p)<--(:clinical_measure_file)) and not ((p)<--(:generic_file))
    OPTIONAL MATCH (sm1)<--(dg:diagnosis)
    with p, sm1, sm, sample_diagnosis_filter_1, apoc.coll.union(COLLECT(DISTINCT {
                        sample_anatomic_site: apoc.text.split(sm1.anatomic_site, ';'),
                        participant_age_at_collection: sm1.participant_age_at_collection,
                        sample_tumor_status: sm1.sample_tumor_status,
                        tumor_classification: sm1.tumor_classification,
                        age_at_diagnosis: dg.age_at_diagnosis,
                        diagnosis_anatomic_site: apoc.text.split(dg.anatomic_site, ';'),
                        disease_phase: dg.disease_phase,
                        diagnosis_classification_system: dg.diagnosis_classification_system,
                        diagnosis_basis: dg.diagnosis_basis,
                        tumor_grade_source: dg.tumor_grade_source,
                        tumor_stage_source: dg.tumor_stage_source,
                        diagnosis: dg.diagnosis
                    }), COLLECT(DISTINCT {
                        sample_anatomic_site: apoc.text.split(sm.anatomic_site, ';'),
                        participant_age_at_collection: sm.participant_age_at_collection,
                        sample_tumor_status: sm.sample_tumor_status,
                        tumor_classification: sm.tumor_classification,
                        age_at_diagnosis: dg.age_at_diagnosis,
                        diagnosis_anatomic_site: apoc.text.split(dg.anatomic_site, ';'),
                        disease_phase: dg.disease_phase,
                        diagnosis_classification_system: dg.diagnosis_classification_system,
                        diagnosis_basis: dg.diagnosis_basis,
                        tumor_grade_source: dg.tumor_grade_source,
                        tumor_stage_source: dg.tumor_stage_source,
                        diagnosis: dg.diagnosis
                    })) AS sample_diagnosis_filter_2
    with p, sm1, sm, apoc.coll.union(sample_diagnosis_filter_1, sample_diagnosis_filter_2) as sample_diagnosis_filter_3
    MATCH (st:study)<--(p)<--(sm1)<-[*2..2]-(sm)
    where not ((sm)<--(:sequencing_file)) and not ((sm)<--(:generic_file)) and not ((sm)<--(:cytogenomic_file)) and not ((sm)<--(:pathology_file)) and not ((sm)<--(:methylation_array_file)) and not ((p)<--(:radiology_file)) and not ((p)<--(:clinical_measure_file)) and not ((p)<--(:generic_file))
    OPTIONAL MATCH (sm)<--(dg:diagnosis)
    with p, sm1, sm, sample_diagnosis_filter_3, apoc.coll.union(COLLECT(DISTINCT {
                        sample_anatomic_site: apoc.text.split(sm1.anatomic_site, ';'),
                        participant_age_at_collection: sm1.participant_age_at_collection,
                        sample_tumor_status: sm1.sample_tumor_status,
                        tumor_classification: sm1.tumor_classification,
                        age_at_diagnosis: dg.age_at_diagnosis,
                        diagnosis_anatomic_site: apoc.text.split(dg.anatomic_site, ';'),
                        disease_phase: dg.disease_phase,
                        diagnosis_classification_system: dg.diagnosis_classification_system,
                        diagnosis_basis: dg.diagnosis_basis,
                        tumor_grade_source: dg.tumor_grade_source,
                        tumor_stage_source: dg.tumor_stage_source,
                        diagnosis: dg.diagnosis
                    }), COLLECT(DISTINCT {
                        sample_anatomic_site: apoc.text.split(sm.anatomic_site, ';'),
                        participant_age_at_collection: sm.participant_age_at_collection,
                        sample_tumor_status: sm.sample_tumor_status,
                        tumor_classification: sm.tumor_classification,
                        age_at_diagnosis: dg.age_at_diagnosis,
                        diagnosis_anatomic_site: apoc.text.split(dg.anatomic_site, ';'),
                        disease_phase: dg.disease_phase,
                        diagnosis_classification_system: dg.diagnosis_classification_system,
                        diagnosis_basis: dg.diagnosis_basis,
                        tumor_grade_source: dg.tumor_grade_source,
                        tumor_stage_source: dg.tumor_stage_source,
                        diagnosis: dg.diagnosis
                    })) AS sample_diagnosis_filter_4
    with p, sm1, sm, apoc.coll.union(sample_diagnosis_filter_3, sample_diagnosis_filter_4) as sample_diagnosis_filter
    MATCH (st:study)<--(p)<--(sm1)<-[*2..2]-(sm)
    OPTIONAL MATCH (p)<-[:of_survival]-(su:survival)
    OPTIONAL MATCH (p)<-[:of_treatment]-(tm:treatment)
    OPTIONAL MATCH (p)<-[:of_treatment_response]-(tr:treatment_response)
    OPTIONAL MATCH (st)<-[:of_study_personnel]-(stp:study_personnel)
    OPTIONAL MATCH (st)<-[:of_study_funding]-(stf:study_funding)
    with distinct p, sm, st, sample_diagnosis_filter, COLLECT(DISTINCT {last_known_survival_status: su.last_known_survival_status, 
              event_free_survival_status: su.event_free_survival_status, 
              first_event: su.first_event,
              age_at_last_known_survival_status: su.age_at_last_known_survival_status} ) AS survival_filters,
            COLLECT(DISTINCT{treatment_type: apoc.text.split(tm.treatment_type, ';'),
            treatment_agent: apoc.text.split(tm.treatment_agent, ';'),
            age_at_treatment_start: tm.age_at_treatment_start}) as treatment_filters,
            COLLECT(DISTINCT{response_category: tr.response_category,
            age_at_response: tr.age_at_response}) as treatment_response_filters , stf, stp
    RETURN DISTINCT
    null as id,
    p.id as pid,
    null AS file_id,
    null AS guid,
    null AS file_name,
    null AS file_access,
    null AS data_category,
    null AS file_type,
    null AS file_mapping_level,
    null AS file_description,
    null AS file_size,
    null AS md5sum,
    st.study_id AS study_id,
    st.dbgap_accession as dbgap_accession,
    st.study_acronym as study_acronym,
    st.study_name as study_name,
    p.participant_id AS participant_id,
    sm.sample_id AS sample_id,
    COLLECT(DISTINCT {
        race: apoc.text.split(p.race, ';'),
        sex_at_birth: p.sex_at_birth
    }) AS participant_filters,
    sample_diagnosis_filter as sample_diagnosis_filters,
    survival_filters as survival_filters,
    treatment_filters as treatment_filters,
    treatment_response_filters as treatment_response_filters,
    null AS library_selection,
    null AS library_source_material,
    null AS library_source_molecule,
    null AS library_strategy
    union all
    with st
    MATCH (st)<--(p:participant)<--(sm:sample)
    where not ((sm)<-[*..3]-(:sequencing_file)) and not ((sm)<-[*..3]-(:generic_file)) and not ((sm)<-[*..3]-(:cytogenomic_file)) and not ((sm)<-[*..3]-(:pathology_file)) and not ((sm)<-[*..3]-(:methylation_array_file)) and not ((p)<--(:radiology_file)) and not ((p)<--(:clinical_measure_file)) and not ((p)<--(:generic_file))
    OPTIONAL MATCH (p)<-[*..2]-(dg:diagnosis)
    OPTIONAL MATCH (p)<-[:of_survival]-(su:survival)
    OPTIONAL MATCH (p)<-[:of_treatment]-(tm:treatment)
    OPTIONAL MATCH (p)<-[:of_treatment_response]-(tr:treatment_response)
    OPTIONAL MATCH (st)<-[:of_study_personnel]-(stp:study_personnel)
    OPTIONAL MATCH (st)<-[:of_study_funding]-(stf:study_funding)
    with sm, p, st, dg, COLLECT(DISTINCT {last_known_survival_status: su.last_known_survival_status, 
              event_free_survival_status: su.event_free_survival_status, 
              first_event: su.first_event,
              age_at_last_known_survival_status: su.age_at_last_known_survival_status} ) AS survival_filters,
            COLLECT(DISTINCT{treatment_type: apoc.text.split(tm.treatment_type, ';'),
            treatment_agent: apoc.text.split(tm.treatment_agent, ';'),
            age_at_treatment_start: tm.age_at_treatment_start}) as treatment_filters,
            COLLECT(DISTINCT{response_category: tr.response_category,
            age_at_response: tr.age_at_response}) as treatment_response_filters , stf, stp
    RETURN DISTINCT
    null as id,
    p.id as pid,
    null AS file_id,
    null AS guid,
    null AS file_name,
    null as file_access,
    null AS data_category,
    null AS file_type,
    null as file_mapping_level,
    null AS file_description,
    null AS file_size,
    null AS md5sum,
    st.study_id AS study_id,
    st.dbgap_accession as dbgap_accession,
    st.study_acronym as study_acronym,
    st.study_name as study_name,
    p.participant_id AS participant_id,
    sm.sample_id AS sample_id,
    COLLECT(DISTINCT {
        race: apoc.text.split(p.race, ';'),
        sex_at_birth: p.sex_at_birth
    }) AS participant_filters,
    COLLECT(DISTINCT {
        sample_anatomic_site: apoc.text.split(sm.anatomic_site, ';'),
        participant_age_at_collection: sm.participant_age_at_collection,
        sample_tumor_status: sm.sample_tumor_status,
        tumor_classification: sm.tumor_classification,
        age_at_diagnosis: dg.age_at_diagnosis,
        diagnosis_anatomic_site: apoc.text.split(dg.anatomic_site, ';'),
        disease_phase: dg.disease_phase,
        diagnosis_classification_system: dg.diagnosis_classification_system,
        diagnosis_basis: dg.diagnosis_basis,
        tumor_grade_source: dg.tumor_grade_source,
        tumor_stage_source: dg.tumor_stage_source,
        diagnosis: dg.diagnosis
    }) AS sample_diagnosis_filters,
    treatment_filters as treatment_filters,
    survival_filters as survival_filters,
    treatment_response_filters as treatment_response_filters,
    null AS library_selection,
    null AS library_source_material,
    null AS library_source_molecule,
    null AS library_strategy
    union all
    with st
    MATCH (st)<--(cl)<--(sm:sample)
    Where (cl: cell_line or cl: pdx) and not ((sm)<--(:sequencing_file)) and not ((sm)<--(:generic_file)) and not ((sm)<--(:cytogenomic_file)) and not ((sm)<--(:pathology_file)) and not ((sm)<--(:methylation_array_file))
    optional match (sm)<--(dg:diagnosis)
    OPTIONAL MATCH (st)<-[:of_study_personnel]-(stp:study_personnel)
    OPTIONAL MATCH (st)<-[:of_study_funding]-(stf:study_funding)
    with sm, dg, st, stf, stp
    RETURN DISTINCT
    null as id,
    null as pid,
    null AS file_id,
    null AS guid,
    null AS file_name,
    null AS file_access,
    null AS data_category,
    null AS file_type,
    null as file_mapping_level,
    null AS file_description,
    null AS file_size,
    null AS md5sum,
    st.study_id AS study_id,
    st.dbgap_accession as dbgap_accession,
    st.study_acronym as study_acronym,
    st.study_name as study_name,
    null AS participant_id,
    sm.sample_id AS sample_id,
    null AS participant_filters,
    null as survival_filters,
    null as treatment_filters,
    null as treatment_response_filters,
    COLLECT(DISTINCT {
        sample_anatomic_site: apoc.text.split(sm.anatomic_site, ';'),
        participant_age_at_collection: sm.participant_age_at_collection,
        sample_tumor_status: sm.sample_tumor_status,
        tumor_classification: sm.tumor_classification,
        age_at_diagnosis: null,
        diagnosis_anatomic_site: null,
        disease_phase: null,
        diagnosis_classification_system: null,
        diagnosis_basis: null,
        tumor_grade_source: null,
        tumor_stage_source: null,
        diagnosis_classification: null
    }) AS sample_diagnosis_filters,
    null AS library_selection,
    null AS library_source_material,
    null AS library_source_molecule,
    null AS library_strategy
    union all
    with st
    MATCH (st)<--(p:participant)
    where not ((p)<--(:sample)) and not ((p)<--(:radiology_file)) and not ((p)<--(:clinical_measure_file)) and not ((p)<--(:generic_file))
    OPTIONAL MATCH (p)<-[:of_diagnosis]-(dg:diagnosis)
    OPTIONAL MATCH (p)<-[:of_survival]-(su:survival)
    OPTIONAL MATCH (p)<-[:of_treatment]-(tm:treatment)
    OPTIONAL MATCH (p)<-[:of_treatment_response]-(tr:treatment_response)
    OPTIONAL MATCH (st)<-[:of_study_personnel]-(stp:study_personnel)
    OPTIONAL MATCH (st)<-[:of_study_funding]-(stf:study_funding)
    with p, st, dg, COLLECT(DISTINCT {last_known_survival_status: su.last_known_survival_status, 
                event_free_survival_status: su.event_free_survival_status, 
                first_event: su.first_event,
                age_at_last_known_survival_status: su.age_at_last_known_survival_status} ) AS survival_filters,
            COLLECT(DISTINCT{treatment_type: apoc.text.split(tm.treatment_type, ';'),
            treatment_agent: apoc.text.split(tm.treatment_agent, ';'),
            age_at_treatment_start: tm.age_at_treatment_start}) as treatment_filters,
            COLLECT(DISTINCT{response_category: tr.response_category,
            age_at_response: tr.age_at_response}) as treatment_response_filters, stf, stp          
    RETURN DISTINCT
    null as id,
    p.id as pid,
    null AS file_id,
    null AS guid,
    null AS file_name,
    null AS file_access,
    null AS data_category,
    null AS file_type,
    null as file_mapping_level,
    null AS file_description,
    null AS file_size,
    null AS md5sum,
    st.study_id AS study_id,
    st.dbgap_accession as dbgap_accession,
    st.study_acronym as study_acronym,
    st.study_name as study_name,
    p.participant_id AS participant_id,
    null AS sample_id,
    COLLECT(DISTINCT {
        race: apoc.text.split(p.race, ';'),
        sex_at_birth: p.sex_at_birth
    }) AS participant_filters,
    COLLECT(DISTINCT {
        sample_anatomic_site: null,
        participant_age_at_collection: null,
        sample_tumor_status: null,
        tumor_classification: null,
        age_at_diagnosis: dg.age_at_diagnosis,
        diagnosis_anatomic_site: apoc.text.split(dg.anatomic_site, ';'),
        disease_phase: dg.disease_phase,
        diagnosis_classification_system: dg.diagnosis_classification_system,
        diagnosis_basis: dg.diagnosis_basis,
        tumor_grade_source: dg.tumor_grade_source,
        tumor_stage_source: dg.tumor_stage_source,
        diagnosis_classification: dg.diagnosis_classification
    }) AS sample_diagnosis_filters, 
    survival_filters as survival_filters,
    treatment_filters as treatment_filters,
    treatment_response_filters as treatment_response_filters,
    null AS library_selection,
    null AS library_source_material,
    null AS library_source_molecule,
    null AS library_strategy
}
with id, guid, file_name, data_category, file_type, file_description, file_size, file_access, file_mapping_level, md5sum, study_id, study_acronym, study_name, participant_id, sample_id, participant_filters, sample_diagnosis_filters, survival_filters, treatment_filters, treatment_response_filters, library_selection,library_source_material, library_source_molecule, library_strategy
unwind participant_filters as participant_filter
with id, guid, file_name, data_category, file_type, file_description, file_size, file_access, file_mapping_level, md5sum, study_id, study_acronym, study_name, participant_id, sample_id, participant_filter, sample_diagnosis_filters, survival_filters, treatment_filters, treatment_response_filters, library_selection,library_source_material, library_source_molecule, library_strategy
where participant_id in [''] and participant_filter.sex_at_birth in [''] and ANY(element IN [''] WHERE element IN participant_filter.race)
unwind sample_diagnosis_filters as sample_diagnosis_filter
unwind survival_filters as survival_filter
unwind treatment_filters as treatment_filter
unwind treatment_response_filters as treatment_response_filter
with id, guid, file_name, data_category, file_type, file_description, file_size, file_access, file_mapping_level, md5sum, study_id, study_acronym, study_name, participant_id, sample_id, sample_diagnosis_filter, survival_filter, treatment_filter, treatment_response_filter, library_selection,library_source_material, library_source_molecule, library_strategy
where sample_diagnosis_filter.age_at_diagnosis >= [''] and sample_diagnosis_filter.age_at_diagnosis <= [''] and sample_diagnosis_filter.diagnosis in [''] and ANY(element IN [''] WHERE element IN sample_diagnosis_filter.diagnosis_anatomic_site) and sample_diagnosis_filter.diagnosis_classification_system in [''] and sample_diagnosis_filter.diagnosis_basis in [''] and sample_diagnosis_filter.disease_phase in ['']
        and sample_diagnosis_filter.participant_age_at_collection >= [''] and sample_diagnosis_filter.participant_age_at_collection <= [''] and ANY(element IN [''] WHERE element IN sample_diagnosis_filter.sample_anatomic_site) and sample_diagnosis_filter.sample_tumor_status in [''] and sample_diagnosis_filter.tumor_classification in ['']
        and survival_filter.last_known_survival_status in [''] and survival_filter.event_free_survival_status in [''] and survival_filter.first_event in ['']
        and survival_filter.age_at_last_known_survival_status >= [''] and survival_filter.age_at_last_known_survival_status <= ['']
        and ANY(element IN [''] WHERE element IN treatment_filter.treatment_type) and ANY(element IN [''] WHERE element IN treatment_filter.treatment_agent)
        and treatment_filter.age_at_treatment_start >= [''] and treatment_filter.age_at_treatment_start <= ['']
        and treatment_response_filter.response_category in [''] and treatment_response_filter.age_at_response >= [''] and treatment_response_filter.age_at_response <= [''] 
with distinct id, guid, file_name, data_category, file_type, file_description, file_size, file_access, file_mapping_level, md5sum, study_id, study_acronym, study_name, participant_id, sample_id, library_selection, library_source_material, library_source_molecule, library_strategy
call {
  with id, guid, file_name, data_category, file_type, file_description, file_size, file_access, file_mapping_level, md5sum, study_id, study_acronym, study_name, participant_id, sample_id,library_selection,library_source_material, library_source_molecule, library_strategy
  return id as fid, guid as dig, file_name as fn, data_category as fc, file_type as ft, file_description as fd, file_size as fsize, file_mapping_level as fml, file_access as fa, md5sum as md5, study_id as sid, study_acronym as sa, study_name as sn, participant_id as pid, sample_id as smid,library_selection as ls, library_source_material as lsma, library_source_molecule as lsmo, library_strategy as listr
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
    apoc.text.split(file.data_category, ';') AS fc,
    file.file_access AS fa,
    file.file_type AS ft,
    file.file_mapping_level AS fml,
    file.file_description AS fd,
    file.file_size AS fsize,
    file.md5sum AS md5,
    stu.study_id AS sid,
    stu.study_acronym as sa,
    stu.study_name as sn,
    null AS pid,
    null AS smid,
    null AS ls,
    null AS lsma,
    null AS lsmo,
    null AS listr
  UNION ALL
  with study_id
  MATCH (file:generic_file)
  MATCH (stu:study)<-[:of_generic_file]-(file)
  where stu.study_id = study_id
  OPTIONAL MATCH (stu)<-[:of_study_personnel]-(stp:study_personnel)
  OPTIONAL MATCH (stu)<-[:of_study_funding]-(stf:study_funding)
  With file, stu, stf, stp
  RETURN DISTINCT
    file.id as fid,
    file.dcf_indexd_guid AS dig,
    file.file_name AS fn,
    apoc.text.split(file.data_category, ';') AS fc,
    file.file_access AS fa,
    file.file_type AS ft,
    file.file_mapping_level AS fml,
    file.file_description AS fd,
    file.file_size AS fsize,
    file.md5sum AS md5,
    stu.study_id AS sid,
    stu.study_acronym as sa,
    stu.study_name as sn,
    null AS pid,
    null AS smid,
    null AS ls,
    null AS lsma,
    null AS lsmo,
    null AS listr
}
with fid as id, dig as guid, fn as file_name, fc as data_category, ft as file_type, fd as file_description, fsize as file_size, fa as file_access, fml as file_mapping_level, md5 as md5sum, sid as study_id, sa as study_acronym, sn as study_name, pid as participant_id, smid as sample_id,ls as library_selection,lsma as library_source_material, lsmo as library_source_molecule, listr as library_strategy
where ANY(element IN [''] WHERE element IN data_category) and file_type in [''] and file_mapping_level in ['']
        and study_acronym in [''] and study_name in [''] 
        and library_selection in [''] and library_source_material in [''] and library_source_molecule in [''] and library_strategy in ['']
with id, guid, file_name, data_category, file_type, file_description, file_size, ['Bytes', 'KB', 'MB', 'GB', 'TB'] AS units,
        toInteger(floor(log(file_size)/log(1024))) as i,
        2 as precision, file_access, file_mapping_level, md5sum, study_id, study_acronym, study_name, participant_id, sample_id,library_selection,library_source_material, library_source_molecule, library_strategy
with id, guid, file_name, data_category, file_type, file_description, file_size, file_size /(1024^i) AS value,
        10^precision AS factor,
        units[i] as unit, file_access, file_mapping_level, md5sum, study_id, study_acronym, study_name, participant_id, sample_id, library_selection,library_source_material, library_source_molecule, library_strategy
with id, guid, file_name, data_category, file_type, file_description, unit,
        round(factor * value)/factor AS size, file_access, file_mapping_level, md5sum, study_id, study_acronym, study_name, participant_id, sample_id, library_selection, library_source_material, library_source_molecule, library_strategy     
with DISTINCT id,
        file_name,
        data_category,
        file_description,
        file_type,
        CASE size % 1 WHEN 0 THEN apoc.convert.toInteger(size)+' ' +unit ELSE size+' ' +unit END AS file_size_new,
        file_access,
        file_mapping_level,
        study_id,
        participant_id,
        sample_id,
        guid,
        md5sum,
        library_selection,
        library_source_material,
        library_source_molecule,
        library_strategy
where id IS NOT NULL
RETURN file_name AS `File Name`,
data_category As `Data Category`,
file_description As `File Description`,
file_type As `File Type`,
file_size_new As `File Size`,
file_access As `File Access`,
study_id As `Study ID`,
coalesce(participant_id, '') As `Participant ID`,
coalesce(sample_id, '') As `Sample ID`,
guid As `GUID`,
md5sum As `MD5Sum`,
coalesce(library_selection, '') As `Library Selection`,
coalesce(library_source_material, '') As `Library Source Material`,
coalesce(library_strategy, '') As `Library Strategy`,
coalesce(library_source_molecule, '') As `Library Source Molecule `,
file_mapping_level As `File Mapping`
ORDER BY file_name LIMIT 100
Match (st:study)
where st.dbgap_accession in ['']
with st
Call {
    with st
    MATCH (file)
    where (file:clinical_measure_file or file:radiology_file)
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
    OPTIONAL MATCH (st)<-[:of_study_personnel]-(stp:study_personnel)
    OPTIONAL MATCH (st)<-[:of_study_funding]-(stf:study_funding)
    RETURN DISTINCT
    file.id as id,
    p.id as pid,
    CASE LABELS(file)[0]
            WHEN 'clinical_measure_file' THEN file.clinical_measure_file_id
            WHEN 'radiology_file' THEN file.radiology_file_id
            ELSE null END AS file_id,
    file.dcf_indexd_guid AS guid,
    file.file_name AS file_name,
    CASE labels(file)[0] WHEN 'clinical_measure_file' THEN 'Clinical data'
                                WHEN 'radiology_file' THEN 'Radiology imaging'
                                ELSE null END AS file_category,
    file.file_type AS file_type,
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
    case when 'Dead' in COLLECT(DISTINCT su.last_known_survival_status) then ['Dead']
            else COLLECT(DISTINCT su.last_known_survival_status) end as last_known_survival_status,      
    null AS library_selection,
    null AS library_source_material,
    null AS library_source_molecule,
    null AS library_strategy
    union all
    with st
    MATCH (file)
    where (file: sequencing_file OR file:pathology_file OR file:methylation_array_file OR file:cytogenomic_file)
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
    OPTIONAL MATCH (st)<-[:of_study_personnel]-(stp:study_personnel)
    OPTIONAL MATCH (st)<-[:of_study_funding]-(stf:study_funding)
    with file, p, sample_diagnosis_filter, sm1, sm, st, COLLECT(DISTINCT su.last_known_survival_status) as vital_status, stf, stp
    RETURN DISTINCT
    file.id as id,
    p.id as pid,
    CASE LABELS(file)[0]
            WHEN 'sequencing_file' THEN file.sequencing_file_id
            WHEN 'cytogenomic_file' THEN file.cytogenomic_file_id
            WHEN 'pathology_file' THEN file.pathology_file_id
            WHEN 'methylation_array_file' THEN file.methylation_array_file_id ELSE null END AS file_id,
    file.dcf_indexd_guid AS guid,
    file.file_name AS file_name,
    CASE LABELS(file)[0] WHEN 'sequencing_file' THEN 'Sequencing'
                            WHEN 'cytogenomic_file' THEN 'Cytogenomic'
                            WHEN 'pathology_file' THEN 'Pathology imaging'
                            WHEN 'methylation_array_file' THEN 'Methylation array' 
                            ELSE null END AS file_category,
    file.file_type AS file_type,
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
    case when 'Dead' in vital_status then ['Dead']
            else vital_status end as last_known_survival_status,
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
    WHERE (file:sequencing_file OR file:pathology_file OR file:methylation_array_file OR file:cytogenomic_file)
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
            WHEN 'cytogenomic_file' THEN file.cytogenomic_file_id
            WHEN 'pathology_file' THEN file.pathology_file_id
            WHEN 'methylation_array_file' THEN file.methylation_array_file_id ELSE null END AS file_id,
    file.dcf_indexd_guid AS guid,
    file.file_name AS file_name,
    CASE LABELS(file)[0]
            WHEN 'sequencing_file' THEN 'Sequencing'
            WHEN 'cytogenomic_file' THEN 'Cytogenomic'
            WHEN 'pathology_file' THEN 'Pathology imaging'
            WHEN 'methylation_array_file' THEN 'Methylation array' ELSE null END AS file_category,
    file.file_type AS file_type,
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
    null as last_known_survival_status,
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
    where not ((sm)<--(:sequencing_file)) and not ((sm)<--(:cytogenomic_file)) and not ((sm)<--(:pathology_file)) and not ((sm)<--(:methylation_array_file)) and not ((p)<--(:radiology_file)) and not ((p)<--(:clinical_measure_file))
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
    where not ((sm)<--(:sequencing_file)) and not ((sm)<--(:cytogenomic_file)) and not ((sm)<--(:pathology_file)) and not ((sm)<--(:methylation_array_file)) and not ((p)<--(:radiology_file)) and not ((p)<--(:clinical_measure_file))
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
    where not ((sm)<--(:sequencing_file)) and not ((sm)<--(:cytogenomic_file)) and not ((sm)<--(:pathology_file)) and not ((sm)<--(:methylation_array_file)) and not ((p)<--(:radiology_file)) and not ((p)<--(:clinical_measure_file))
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
    OPTIONAL MATCH (st)<-[:of_study_personnel]-(stp:study_personnel)
    OPTIONAL MATCH (st)<-[:of_study_funding]-(stf:study_funding)
    with distinct p, sm, st, sample_diagnosis_filter, COLLECT(DISTINCT su.last_known_survival_status) as vital_status, stf, stp
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
    st.dbgap_accession as dbgap_accession,
    st.study_acronym as study_acronym,
    st.study_name as study_name,
    p.participant_id AS participant_id,
    sm.sample_id AS sample_id,
    COLLECT(DISTINCT {
        race: apoc.text.split(p.race, ';'),
        sex_at_birth: p.sex_at_birth
    }) AS participant_filters,
    case when 'Dead' in vital_status then ['Dead']
            else vital_status end as last_known_survival_status,         
    sample_diagnosis_filter AS sample_diagnosis_filters,
    null AS library_selection,
    null AS library_source_material,
    null AS library_source_molecule,
    null AS library_strategy
    union all
    with st
    MATCH (st)<--(p:participant)<--(sm:sample)
    where not ((sm)<-[*..3]-(:sequencing_file)) and not ((sm)<-[*..3]-(:cytogenomic_file)) and not ((sm)<-[*..3]-(:pathology_file)) and not ((sm)<-[*..3]-(:methylation_array_file)) and not ((p)<--(:radiology_file)) and not ((p)<--(:clinical_measure_file))
    OPTIONAL MATCH (p)<-[*..2]-(dg:diagnosis)
    OPTIONAL MATCH (p)<-[:of_survival]-(su:survival)
    OPTIONAL MATCH (st)<-[:of_study_personnel]-(stp:study_personnel)
    OPTIONAL MATCH (st)<-[:of_study_funding]-(stf:study_funding)
    with sm, p, st, dg, COLLECT(DISTINCT su.last_known_survival_status) as vital_status, stf, stp
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
    case when 'Dead' in vital_status then ['Dead']
            else vital_status end as last_known_survival_status,
    null AS library_selection,
    null AS library_source_material,
    null AS library_source_molecule,
    null AS library_strategy
    union all
    with st
    MATCH (st)<--(cl)<--(sm:sample)
    Where (cl: cell_line or cl: pdx) and not ((sm)<--(:sequencing_file)) and not ((sm)<--(:cytogenomic_file)) and not ((sm)<--(:pathology_file)) and not ((sm)<--(:methylation_array_file))
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
    null AS file_category,
    null AS file_type,
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
    null as last_known_survival_status,
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
    where not ((p)<--(:sample)) and not ((p)<--(:radiology_file)) and not ((p)<--(:clinical_measure_file))
    OPTIONAL MATCH (p)<-[:of_diagnosis]-(dg:diagnosis)
    OPTIONAL MATCH (p)<-[:of_survival]-(su:survival)
    OPTIONAL MATCH (st)<-[:of_study_personnel]-(stp:study_personnel)
    OPTIONAL MATCH (st)<-[:of_study_funding]-(stf:study_funding)
    with p, st, dg, COLLECT(DISTINCT su.last_known_survival_status) as vital_status, stf, stp
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
    case when 'Dead' in vital_status then ['Dead']
            else vital_status end as last_known_survival_status,
    null AS library_selection,
    null AS library_source_material,
    null AS library_source_molecule,
    null AS library_strategy
}
with id, guid, file_name, file_category, file_type, file_description, file_size, md5sum, study_id, study_acronym, study_name, pid, participant_id, sample_id, case participant_filters when null then [null] else participant_filters end as participant_filters, sample_diagnosis_filters, last_known_survival_status, library_selection,library_source_material, library_source_molecule, library_strategy
unwind participant_filters as participant_filter
with id, guid, file_name, file_category, file_type, file_description, file_size, md5sum, study_id, study_acronym, study_name, pid, participant_id, sample_id, participant_filter, sample_diagnosis_filters, last_known_survival_status, library_selection,library_source_material, library_source_molecule, library_strategy
where participant_id in [''] and participant_filter.sex_at_birth in [''] and ANY(element IN [''] WHERE element IN participant_filter.race)
unwind sample_diagnosis_filters as sample_diagnosis_filter
with id, guid, file_name, file_category, file_type, file_description, file_size, md5sum, study_id, study_acronym, study_name, pid, participant_id, sample_id, sample_diagnosis_filter, last_known_survival_status, library_selection,library_source_material, library_source_molecule, library_strategy
where sample_diagnosis_filter.age_at_diagnosis >= [''] and sample_diagnosis_filter.age_at_diagnosis <= [''] and sample_diagnosis_filter.diagnosis in [''] and ANY(element IN [''] WHERE element IN sample_diagnosis_filter.diagnosis_anatomic_site) and sample_diagnosis_filter.diagnosis_classification_system in [''] and sample_diagnosis_filter.diagnosis_basis in [''] and sample_diagnosis_filter.disease_phase in ['']
        and sample_diagnosis_filter.participant_age_at_collection >= [''] and sample_diagnosis_filter.participant_age_at_collection <= [''] and ANY(element IN [''] WHERE element IN sample_diagnosis_filter.sample_anatomic_site) and sample_diagnosis_filter.sample_tumor_status in [''] and sample_diagnosis_filter.tumor_classification in ['']
with id, guid, file_name, file_category, file_type, file_description, file_size, md5sum, study_id, study_acronym, study_name, pid, participant_id, sample_id, last_known_survival_status, library_selection,library_source_material, library_source_molecule, library_strategy
where ANY(element IN [''] WHERE element IN last_known_survival_status)
with distinct id, guid, file_name, file_category, file_type, file_description, file_size, md5sum, study_id, study_acronym, study_name, pid, participant_id, sample_id, library_selection, library_source_material, library_source_molecule, library_strategy
call {
  with id, guid, file_name, file_category, file_type, file_description, file_size, md5sum, study_id, study_acronym, study_name, pid, participant_id, sample_id,library_selection,library_source_material, library_source_molecule, library_strategy
  return id as fid, guid as dig, file_name as fn, file_category as fc, file_type as ft, file_description as fd, file_size as fsize, md5sum as md5, study_id as sid, study_acronym as sa, study_name as sn, pid as u_p_id, participant_id as p_id, sample_id as smid,library_selection as ls, library_source_material as lsma, library_source_molecule as lsmo, library_strategy as listr
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
    stu.study_acronym as sa,
    stu.study_name as sn,
    null AS p_id,
    null AS u_p_id,
    null AS smid,
    null AS ls,
    null AS lsma,
    null AS lsmo,
    null AS listr
}
with fid as id, dig as guid, fn as file_name, fc as file_category, ft as file_type, fd as file_description, fsize as file_size, md5 as md5sum, sid as study_id, sa as study_acronym, sn as study_name, u_p_id as unique_participant_id, p_id as participant_id, smid as sample_id,ls as library_selection,lsma as library_source_material, lsmo as library_source_molecule, listr as library_strategy
where file_category in [''] and file_type in [''] 
        and study_acronym in [''] and study_name in [''] 
        and library_selection in [''] and library_source_material in [''] and library_source_molecule in [''] and library_strategy in ['']
with DISTINCT id, study_id, unique_participant_id, sample_id
with id, study_id, unique_participant_id, case sample_id when null then [null] else apoc.text.split(sample_id, ',') end as sample_ids
unwind sample_ids as single_sample_id
return count(distinct study_id) as Studies, count(distinct unique_participant_id) as Participants, count(distinct single_sample_id) as Samples, count(distinct id) as Files
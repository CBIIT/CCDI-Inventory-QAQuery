Match (st:study)
where st.dbgap_accession in [''] and st.study_status in ['']
with st
Call {
  with st
  MATCH (st)<--(p:participant)<--(dg:diagnosis)
  WITH distinct p, {
            pid: p.id,
            participant_id: p.participant_id,
            race: apoc.text.split(p.race, ';'),
            sex_at_birth: p.sex_at_birth
          } AS opensearch_data
  optional MATCH (p)<-[:of_sample]-(sm1:sample)<--(cl)<--(sm2:sample)
  WHERE (cl: cell_line or cl: pdx)
  optional Match (sm2)<--(file)
  WHERE (file: sequencing_file or file: generic_file OR file:pathology_file OR file:methylation_array_file OR file:cytogenomic_file)
  with p, opensearch_data, case COLLECT(distinct sm1) when [] then []
                else COLLECT(DISTINCT {
                        sample_anatomic_site: apoc.text.split(sm1.anatomic_site, ';'),
                        participant_age_at_collection: sm1.participant_age_at_collection,
                        sample_tumor_status: sm1.sample_tumor_status,
                        tumor_classification: sm1.tumor_classification,
                        data_category: apoc.text.split(file.data_category, ';'),
                        file_type: CASE LABELS(file)[0]
                                  When null then null
                                  else file.file_type end,
                        file_mapping_level: file.file_mapping_level,
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
                        sample_anatomic_site: apoc.text.split(sm2.anatomic_site, ';'),
                        participant_age_at_collection: sm2.participant_age_at_collection,
                        sample_tumor_status: sm2.sample_tumor_status,
                        tumor_classification: sm2.tumor_classification,
                        data_category: apoc.text.split(file.data_category, ';'),
                        file_type: CASE LABELS(file)[0]
                                  When null then null
                                  else file.file_type end,
                        file_mapping_level: file.file_mapping_level,
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
  with p, opensearch_data, apoc.coll.union(sample1,sample2) as cell_line_pdx_file_filters_1
  optional MATCH (p)<-[:of_sample]-(sm1:sample)<--(cl)<--(sm2:sample)
  WHERE (cl: cell_line or cl: pdx)
  optional Match (sm1)<--(file)
  WHERE (file: sequencing_file or file: generic_file OR file:pathology_file OR file:methylation_array_file OR file:cytogenomic_file)
  with p, opensearch_data, cell_line_pdx_file_filters_1, case COLLECT(distinct sm2) when [] then []
                    else COLLECT(DISTINCT {
                        sample_anatomic_site: apoc.text.split(sm2.anatomic_site, ';'),
                        participant_age_at_collection: sm2.participant_age_at_collection,
                        sample_tumor_status: sm2.sample_tumor_status,
                        tumor_classification: sm2.tumor_classification,
                        data_category: apoc.text.split(file.data_category, ';'),
                        file_type: CASE LABELS(file)[0]
                                  When null then null
                                  else file.file_type end,
                        file_mapping_level: file.file_mapping_level,
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
                    }) end AS cell_line_pdx_file_filters_2
  with p, opensearch_data, apoc.coll.union(cell_line_pdx_file_filters_1, cell_line_pdx_file_filters_2) as cell_line_pdx_file_filters
  OPTIONAL MATCH (p)<-[:of_sample]-(sm:sample)
  optional match (sm)<--(file)
  WHERE (file: sequencing_file or file: generic_file OR file:pathology_file OR file:methylation_array_file OR file:cytogenomic_file)
  with p, opensearch_data, cell_line_pdx_file_filters, COLLECT(DISTINCT {
                sample_anatomic_site: apoc.text.split(sm.anatomic_site, ';'),
                participant_age_at_collection: sm.participant_age_at_collection,
                sample_tumor_status: sm.sample_tumor_status,
                tumor_classification: sm.tumor_classification,
                data_category: apoc.text.split(file.data_category, ';'),
                file_type: file.file_type,
                file_mapping_level: file.file_mapping_level,
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
            }) AS general_file_filters_1
  with p, opensearch_data, apoc.coll.union(cell_line_pdx_file_filters, general_file_filters_1) as general_file_filters
  OPTIONAL Match (p)<-[*..3]-(sm:sample)
  OPTIONAL MATCH (p)<--(file)
  where (file:clinical_measure_file or file: generic_file or file:radiology_file)
  with p, opensearch_data, general_file_filters, case COLLECT(file) when [] then [] else COLLECT(DISTINCT {
            sample_anatomic_site: apoc.text.split(sm.anatomic_site, ';'),
            participant_age_at_collection: sm.participant_age_at_collection,
            sample_tumor_status: sm.sample_tumor_status,
            tumor_classification: sm.tumor_classification,
            data_category: apoc.text.split(file.data_category, ';'),
            file_type: file.file_type,
            file_mapping_level: file.file_mapping_level,
            library_selection: null,
            library_source_material: null,
            library_source_molecule: null,
            library_strategy: null
    }) end as participant_file_filters
  optional match (st:study)<--(p)<-[*..3]-(sm:sample)
  OPTIONAL MATCH (st)<--(file)
  where (file: clinical_measure_file  or file: generic_file)
  with p, opensearch_data, general_file_filters, participant_file_filters, case COLLECT(file) when [] then [] else COLLECT(DISTINCT {
            sample_anatomic_site: apoc.text.split(sm.anatomic_site, ';'),
            participant_age_at_collection: sm.participant_age_at_collection,
            sample_tumor_status: sm.sample_tumor_status,
            tumor_classification: sm.tumor_classification,
            data_category: apoc.text.split(file.data_category, ';'),
            file_type: file.file_type,
            file_mapping_level: file.file_mapping_level,
            library_selection: null,
            library_source_material: null,
            library_source_molecule: null,
            library_strategy: null
    }) end as study_file_filters
  with p, apoc.map.merge(opensearch_data, {
    sample_file_filters: general_file_filters + apoc.coll.union(participant_file_filters, study_file_filters)
  }) AS opensearch_data
  OPTIONAL MATCH (p)<-[:of_survival]-(su:survival)
  with p, apoc.map.merge(opensearch_data, {
    survival_filters: COLLECT(DISTINCT {last_known_survival_status: su.last_known_survival_status,
      event_free_survival_status: su.event_free_survival_status,
      first_event: su.first_event,
      age_at_last_known_survival_status: su.age_at_last_known_survival_status})
  }) AS opensearch_data
  OPTIONAL MATCH (p)<-[:of_treatment]-(tm:treatment)
  with p, apoc.map.merge(opensearch_data, {
    treatment_filters: COLLECT(DISTINCT{treatment_type: apoc.text.split(tm.treatment_type, ';'),
    treatment_agent: apoc.text.split(tm.treatment_agent, ';'),
    age_at_treatment_start: tm.age_at_treatment_start})
  }) AS opensearch_data
  OPTIONAL MATCH (p)<-[:of_treatment_response]-(tr:treatment_response)
  with p, apoc.map.merge(opensearch_data, {
    treatment_response_filters: COLLECT(DISTINCT{response_category: tr.response_category,
    age_at_response: tr.age_at_response})
  }) AS opensearch_data
  OPTIONAL MATCH (p)<-[*..4]-(file)
  WHERE (file:clinical_measure_file or file: generic_file OR file: sequencing_file OR file:pathology_file OR file:radiology_file OR file:methylation_array_file OR file:cytogenomic_file)
  with p, apoc.map.merge(opensearch_data, {
              file_count: COUNT(DISTINCT file.id),
              files: COLLECT(DISTINCT file.id)
            }) AS opensearch_data
  MATCH (dg:diagnosis)
  MATCH (p)<-[:of_diagnosis]-(dg)
  OPTIONAL MATCH (st:study)<-[:of_participant]-(p)
  WITH dg, p, opensearch_data , st
  RETURN DISTINCT
    dg.id as id,
    opensearch_data.pid as pid,
    null as sid,
    null as sample_id,
    dg.diagnosis_id as diagnosis_id,
    dg.diagnosis as diagnosis,
    dg.disease_phase as disease_phase,
    dg.diagnosis_classification_system as diagnosis_classification_system,
    dg.diagnosis_basis as diagnosis_basis,
    dg.tumor_grade_source as tumor_grade_source,
    dg.tumor_stage_source as tumor_stage_source,
    apoc.text.split(dg.anatomic_site, ';') as diagnosis_anatomic_site,
    dg.anatomic_site as diagnosis_anatomic_site_str,
    dg.age_at_diagnosis as age_at_diagnosis,
    opensearch_data.participant_id as participant_id,
    opensearch_data.race as race,
    opensearch_data.sex_at_birth as sex_at_birth,
    st.study_id as study_id,
    st.dbgap_accession as dbgap_accession,
    st.study_acronym as study_acronym,
    st.study_name as study_name,
    opensearch_data.survival_filters as survival_filters, 
    opensearch_data.treatment_filters as treatment_filters,
    opensearch_data.treatment_response_filters as treatment_response_filters,  
    opensearch_data.sample_file_filters AS sample_file_filters
  union all
  with st
  match (st)<--(p:participant)<--(sm:sample)<--(dg:diagnosis)
  with distinct dg, {
    id: dg.id,
    pid: p.id,
    sid: sm.id,
    sample_id: sm.sample_id,
    diagnosis_id: dg.diagnosis_id,
    diagnosis: dg.diagnosis,
    disease_phase: dg.disease_phase,
    diagnosis_classification_system: dg.diagnosis_classification_system,
    diagnosis_basis: dg.diagnosis_basis,
    tumor_grade_source: dg.tumor_grade_source,
    tumor_stage_source: dg.tumor_stage_source,
    diagnosis_anatomic_site: apoc.text.split(dg.anatomic_site, ';'),
    diagnosis_anatomic_site_str: dg.anatomic_site,
    age_at_diagnosis: dg.age_at_diagnosis,
    participant_id: p.participant_id,
    race: apoc.text.split(p.race, ';'),
    sex_at_birth: p.sex_at_birth,
    study_id: st.study_id,
    dbgap_accession: st.dbgap_accession,
    study_acronym: st.study_acronym,
    study_name: st.study_name
  } as opensearch_data
  match (p:participant)<--(sm:sample)<--(dg)
  optional match (sm)<-[*..3]-(file)
  where (file: sequencing_file or file: generic_file OR file:pathology_file OR file:methylation_array_file OR file:cytogenomic_file)
  with dg, opensearch_data, COLLECT(DISTINCT {
            sample_anatomic_site: apoc.text.split(sm.anatomic_site, ';'),
            participant_age_at_collection: sm.participant_age_at_collection,
            sample_tumor_status: sm.sample_tumor_status,
            tumor_classification: sm.tumor_classification,
            data_category: apoc.text.split(file.data_category, ';'),
            file_type: file.file_type,
            file_mapping_level: file.file_mapping_level,
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
  where (file: sequencing_file or file: generic_file OR file:pathology_file OR file:methylation_array_file OR file:cytogenomic_file)
  with dg, opensearch_data, sample_file_filter_1, COLLECT(DISTINCT {
            sample_anatomic_site: apoc.text.split(sm.anatomic_site, ';'),
            participant_age_at_collection: sm.participant_age_at_collection,
            sample_tumor_status: sm.sample_tumor_status,
            tumor_classification: sm.tumor_classification,
            data_category: apoc.text.split(file.data_category, ';'),
            file_type: file.file_type,
            file_mapping_level: file.file_mapping_level,
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
  with dg, opensearch_data, apoc.coll.union(sample_file_filter_1, sample_file_filter_2) as sample_file_filter
  match (p:participant)<--(sm1:sample)<--(dg)
  optional match (sm1)<--(cl)<--(sm:sample)
  where (cl: cell_line or cl: pdx)
  optional match (sm1)<--(file)
  where (file: sequencing_file or file: generic_file OR file:pathology_file OR file:methylation_array_file OR file:cytogenomic_file)
  with dg, opensearch_data, sample_file_filter, COLLECT(DISTINCT {
            sample_anatomic_site: apoc.text.split(sm.anatomic_site, ';'),
            participant_age_at_collection: sm.participant_age_at_collection,
            sample_tumor_status: sm.sample_tumor_status,
            tumor_classification: sm.tumor_classification,
            data_category: apoc.text.split(file.data_category, ';'),
            file_type: file.file_type,
            file_mapping_level: file.file_mapping_level,
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
    }) as sample_file_filter_3
  with dg, opensearch_data, apoc.coll.union(sample_file_filter, sample_file_filter_3) as sample_file_filters
  match (p:participant)<--(sm:sample)<--(dg)
  OPTIONAL MATCH (p)<--(file)
  where (file:clinical_measure_file or file: generic_file or file:radiology_file)
  with dg, opensearch_data, sample_file_filters, case COLLECT(file) when [] then [] else COLLECT(DISTINCT {
            sample_anatomic_site: apoc.text.split(sm.anatomic_site, ';'),
            participant_age_at_collection: sm.participant_age_at_collection,
            sample_tumor_status: sm.sample_tumor_status,
            tumor_classification: sm.tumor_classification,
            data_category: apoc.text.split(file.data_category, ';'),
            file_type: file.file_type,
            file_mapping_level: file.file_mapping_level,
            library_selection: null,
            library_source_material: null,
            library_source_molecule: null,
            library_strategy: null
    }) end as participant_file_filters
  match (st:study)<--(p:participant)<--(sm:sample)<--(dg)
  OPTIONAL MATCH (st)<--(file)
  where (file: clinical_measure_file or file: generic_file)
  with dg, opensearch_data, sample_file_filters, participant_file_filters, case COLLECT(file) when [] then [] else COLLECT(DISTINCT {
            sample_anatomic_site: apoc.text.split(sm.anatomic_site, ';'),
            participant_age_at_collection: sm.participant_age_at_collection,
            sample_tumor_status: sm.sample_tumor_status,
            tumor_classification: sm.tumor_classification,
            data_category: apoc.text.split(file.data_category, ';'),
            file_type: file.file_type,
            file_mapping_level: file.file_mapping_level,
            library_selection: null,
            library_source_material: null,
            library_source_molecule: null,
            library_strategy: null
    }) end as study_file_filters
  with dg, opensearch_data, sample_file_filters, apoc.coll.union(participant_file_filters, study_file_filters) as non_sample_file_filters
  with dg, apoc.map.merge(opensearch_data, {
    sample_file_filters: sample_file_filters + non_sample_file_filters
  }) AS opensearch_data
  match (p:participant)<--(sm:sample)<--(dg)
  OPTIONAL MATCH (p)<-[:of_survival]-(su:survival)
  with dg, apoc.map.merge(opensearch_data, {
    survival_filters: COLLECT(DISTINCT {last_known_survival_status: su.last_known_survival_status,
      event_free_survival_status: su.event_free_survival_status,
      first_event: su.first_event,
      age_at_last_known_survival_status: su.age_at_last_known_survival_status})
  }) AS opensearch_data
  match (p:participant)<--(sm:sample)<--(dg)
  OPTIONAL MATCH (p)<-[:of_treatment]-(tm:treatment)
  with dg, apoc.map.merge(opensearch_data, {
    treatment_filters: COLLECT(DISTINCT{treatment_type: apoc.text.split(tm.treatment_type, ';'),
    treatment_agent: apoc.text.split(tm.treatment_agent, ';'),
    age_at_treatment_start: tm.age_at_treatment_start})
  }) AS opensearch_data
  match (p:participant)<--(sm:sample)<--(dg)
  OPTIONAL MATCH (p)<-[:of_treatment_response]-(tr:treatment_response)
  with dg, apoc.map.merge(opensearch_data, {
    treatment_response_filters: COLLECT(DISTINCT{response_category: tr.response_category,
    age_at_response: tr.age_at_response})
  }) AS opensearch_data
  match (p:participant)<--(sm:sample)<--(dg)
  optional match (sm)<--(file)
  WHERE (file: sequencing_file or file: generic_file OR file:pathology_file OR file:methylation_array_file OR file:cytogenomic_file)
  with dg, apoc.map.merge(opensearch_data, {
              file_count: COUNT(DISTINCT file.id),
              files: COLLECT(DISTINCT file.id)
            }) AS opensearch_data         
  RETURN distinct
    opensearch_data.id as id,
    opensearch_data.pid as pid,
    opensearch_data.sid as sid,
    opensearch_data.sample_id as sample_id,
    opensearch_data.diagnosis_id as diagnosis_id,
    opensearch_data.diagnosis as diagnosis,
    opensearch_data.disease_phase as disease_phase,
    opensearch_data.diagnosis_classification_system as diagnosis_classification_system,
    opensearch_data.diagnosis_basis as diagnosis_basis,
    opensearch_data.tumor_grade_source as tumor_grade_source,
    opensearch_data.tumor_stage_source as tumor_stage_source,
    opensearch_data.diagnosis_anatomic_site as diagnosis_anatomic_site,
    opensearch_data.diagnosis_anatomic_site_str as diagnosis_anatomic_site_str,
    opensearch_data.age_at_diagnosis as age_at_diagnosis,
    opensearch_data.participant_id as participant_id,
    opensearch_data.race as race,
    opensearch_data.sex_at_birth as sex_at_birth,
    opensearch_data.study_id as study_id,
    opensearch_data.dbgap_accession as dbgap_accession,
    opensearch_data.study_acronym as study_acronym,
    opensearch_data.study_name as study_name,
    opensearch_data.survival_filters as survival_filters, 
    opensearch_data.treatment_filters as treatment_filters,
    opensearch_data.treatment_response_filters as treatment_response_filters,     
    opensearch_data.sample_file_filters AS sample_file_filters
  union all
  with st
  MATCH (st)<--(p:participant)<-[:of_sample]-(sm1:sample)<--(cl)<--(sm2:sample)<--(dg:diagnosis)
  WHERE (cl: cell_line or cl: pdx)
  optional Match (sm1)<--(file1)
  WHERE (file1: sequencing_file or file1: generic_file OR file1:pathology_file OR file1:methylation_array_file OR file1:cytogenomic_file)
  optional Match (sm2)<--(file2)
  WHERE (file2: sequencing_file or file2: generic_file OR file2:pathology_file OR file2:methylation_array_file OR file2:cytogenomic_file)
  with dg, collect(distinct sm1) as sm1_list, collect(distinct sm2) as sm2_list, collect(distinct file1) as file1_list, collect(distinct file2) as file2_list
  with dg, apoc.coll.union(sm1_list, sm2_list) as samples, apoc.coll.union(file1_list, file2_list) as files
  unwind samples as sample
  with dg, sample, files
  UNWIND (case files when [] then [null] else files end)  AS file
  with dg, sample, file
  with dg, COLLECT(DISTINCT {
            sample_anatomic_site: apoc.text.split(sample.anatomic_site, ';'),
            participant_age_at_collection: sample.participant_age_at_collection,
            sample_tumor_status: sample.sample_tumor_status,
            tumor_classification: sample.tumor_classification,
            data_category: apoc.text.split(file.data_category, ';'),
            file_type: file.file_type,
            file_mapping_level: file.file_mapping_level,
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
  OPTIONAL MATCH (p)<--(file)
  where (file:clinical_measure_file or file: generic_file or file:radiology_file)
  with dg, sample_file_filter, case COLLECT(file) when [] then [] else COLLECT(DISTINCT {
            sample_anatomic_site: apoc.text.split(sm.anatomic_site, ';'),
            participant_age_at_collection: sm.participant_age_at_collection,
            sample_tumor_status: sm.sample_tumor_status,
            tumor_classification: sm.tumor_classification,
            data_category: apoc.text.split(file.data_category, ';'),
            file_type: file.file_type,
            file_mapping_level: file.file_mapping_level,
            library_selection: null,
            library_source_material: null,
            library_source_molecule: null,
            library_strategy: null
    }) end as participant_file_filters_1,
    case COLLECT(file) when [] then [] else COLLECT(DISTINCT {
            sample_anatomic_site: apoc.text.split(sm1.anatomic_site, ';'),
            participant_age_at_collection: sm1.participant_age_at_collection,
            sample_tumor_status: sm1.sample_tumor_status,
            tumor_classification: sm1.tumor_classification,
            data_category: apoc.text.split(file.data_category, ';'),
            file_type: file.file_type,
            file_mapping_level: file.file_mapping_level,
            library_selection: null,
            library_source_material: null,
            library_source_molecule: null,
            library_strategy: null
    }) end as participant_file_filters_2
  with dg, sample_file_filter, apoc.coll.union(participant_file_filters_1, participant_file_filters_2) as participant_file_filters
  optional match (st:study)<--(p)<-[:of_sample]-(sm1:sample)<--(cl)<--(sm:sample)<--(dg)
  OPTIONAL MATCH (st)<--(file)
  where (file: clinical_measure_file or file: generic_file)
  with dg, sample_file_filter, participant_file_filters, case COLLECT(file) when [] then [] else COLLECT(DISTINCT {
            sample_anatomic_site: apoc.text.split(sm.anatomic_site, ';'),
            participant_age_at_collection: sm.participant_age_at_collection,
            sample_tumor_status: sm.sample_tumor_status,
            tumor_classification: sm.tumor_classification,
            data_category: apoc.text.split(file.data_category, ';'),
            file_type: file.file_type,
            file_mapping_level: file.file_mapping_level,
            library_selection: null,
            library_source_material: null,
            library_source_molecule: null,
            library_strategy: null
    }) end as study_file_filters_1, case COLLECT(file) when [] then [] else COLLECT(DISTINCT {
            sample_anatomic_site: apoc.text.split(sm1.anatomic_site, ';'),
            participant_age_at_collection: sm1.participant_age_at_collection,
            sample_tumor_status: sm1.sample_tumor_status,
            tumor_classification: sm1.tumor_classification,
            data_category: apoc.text.split(file.data_category, ';'),
            file_type: file.file_type,
            file_mapping_level: file.file_mapping_level,
            library_selection: null,
            library_source_material: null,
            library_source_molecule: null,
            library_strategy: null
    }) end as study_file_filters_2
  with dg, sample_file_filter, participant_file_filters, apoc.coll.union(study_file_filters_1, study_file_filters_2) as study_file_filters
  with dg, sample_file_filter + apoc.coll.union(participant_file_filters, study_file_filters) as sample_file_filters
  optional match (p:participant)<-[:of_sample]-(sm1:sample)<--(cl)<--(sm:sample)<--(dg)
  WHERE (cl: cell_line or cl: pdx)
  optional match (sm)<--(file)
  WHERE (file: sequencing_file or file: generic_file OR file:pathology_file OR file:methylation_array_file OR file:cytogenomic_file)
  with dg, sample_file_filters, collect(distinct file.id) as files, apoc.coll.union(collect(distinct sm1.id), collect(distinct sm.id)) as sid, apoc.coll.union(collect(distinct sm1.sample_id), collect(distinct sm.sample_id))  as sample_id
  optional match (p:participant)<-[*..4]-(dg)
  OPTIONAL MATCH (p)<-[:of_survival]-(su:survival)
  OPTIONAL MATCH (p)<-[:of_treatment]-(tm:treatment)
  OPTIONAL MATCH (p)<-[:of_treatment_response]-(tr:treatment_response)
  OPTIONAL MATCH (st:study)<-[:of_participant]-(p)
  OPTIONAL MATCH (st)<-[:of_study_personnel]-(stp:study_personnel)
  OPTIONAL MATCH (st)<-[:of_study_funding]-(stf:study_funding)
  WITH dg, p, sid, sample_id, sample_file_filters, files, COLLECT(DISTINCT {last_known_survival_status: su.last_known_survival_status, 
      event_free_survival_status: su.event_free_survival_status, 
      first_event: su.first_event,
      age_at_last_known_survival_status: su.age_at_last_known_survival_status} ) AS survival_filters,
    COLLECT(DISTINCT{treatment_type: apoc.text.split(tm.treatment_type, ';'),
    treatment_agent: apoc.text.split(tm.treatment_agent, ';'),
    age_at_treatment_start: tm.age_at_treatment_start}) as treatment_filters,
    COLLECT(DISTINCT{response_category: tr.response_category,
    age_at_response: tr.age_at_response}) as treatment_response_filters, st, stf, stp          
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
    apoc.text.split(dg.anatomic_site, ';') as diagnosis_anatomic_site,
    dg.anatomic_site as diagnosis_anatomic_site_str,
    dg.age_at_diagnosis as age_at_diagnosis,
    p.participant_id as participant_id,
    apoc.text.split(p.race, ';') as race,
    p.sex_at_birth as sex_at_birth,
    st.study_id as study_id,
    st.dbgap_accession as dbgap_accession,
    st.study_acronym as study_acronym,
    st.study_name as study_name,
    survival_filters as survival_filters,
    treatment_filters as treatment_filters,
    treatment_response_filters as treatment_response_filters,    
    sample_file_filters AS sample_file_filters
  union all
  with st
  match (st)<--(cl)<--(sm:sample)<--(dg:diagnosis)
  where (cl: cell_line or cl: pdx)
  optional match (sm)<--(file)
  where (file: sequencing_file or file: generic_file OR file:pathology_file OR file:methylation_array_file OR file:cytogenomic_file)
  with dg, COLLECT(DISTINCT {
            sample_anatomic_site: apoc.text.split(sm.anatomic_site, ';'),
            participant_age_at_collection: sm.participant_age_at_collection,
            sample_tumor_status: sm.sample_tumor_status,
            tumor_classification: sm.tumor_classification,
            data_category: apoc.text.split(file.data_category, ';'),
            file_type: file.file_type,
            file_mapping_level: file.file_mapping_level,
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
  OPTIONAL MATCH (st)<--(file)
  where (file: clinical_measure_file or file: generic_file)
  with dg, sample_file_filter, case COLLECT(file) when [] then [] else COLLECT(DISTINCT {
            sample_anatomic_site: apoc.text.split(sm.anatomic_site, ';'),
            participant_age_at_collection: sm.participant_age_at_collection,
            sample_tumor_status: sm.sample_tumor_status,
            tumor_classification: sm.tumor_classification,
            data_category: apoc.text.split(file.data_category, ';'),
            file_type: file.file_type,
            file_mapping_level: file.file_mapping_level,
            library_selection: null,
            library_source_material: null,
            library_source_molecule: null,
            library_strategy: null
    }) end as study_file_filters
  with dg, sample_file_filter + study_file_filters as sample_file_filters
  optional match (st:study)<--(cl)<--(sm:sample)<--(dg)
  where (cl: cell_line or cl: pdx)
  optional match (sm)<--(file)
  where (file: sequencing_file or file: generic_file OR file:pathology_file OR file:methylation_array_file OR file:cytogenomic_file)
  OPTIONAL MATCH (st)<-[:of_study_personnel]-(stp:study_personnel)
  OPTIONAL MATCH (st)<-[:of_study_funding]-(stf:study_funding)
  WITH dg, sm, sample_file_filters, file, st, stf, stp
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
    apoc.text.split(dg.anatomic_site, ';') as diagnosis_anatomic_site,
    dg.anatomic_site as diagnosis_anatomic_site_str,
    dg.age_at_diagnosis as age_at_diagnosis,
    null as participant_id,
    null as race,
    null as sex_at_birth,
    st.study_id as study_id,
    st.dbgap_accession as dbgap_accession,
    st.study_acronym as study_acronym,
    st.study_name as study_name,
    COLLECT(DISTINCT {
        last_known_survival_status: null,
        age_at_last_known_survival_status: null,
        event_free_survival_status: null,
        first_event: null
    }) AS survival_filters,
    COLLECT(DISTINCT{treatment_type: null,
    treatment_agent: null,
    age_at_treatment_start: null}) as treatment_filters,
    COLLECT(DISTINCT{response_category: null,
    age_at_response: null}) as treatment_response_filters, 
    sample_file_filters AS sample_file_filters
}
with id, participant_id, sample_id, diagnosis_id, dbgap_accession, study_acronym, study_name, sex_at_birth, race, age_at_diagnosis, diagnosis, diagnosis_anatomic_site, diagnosis_anatomic_site_str, diagnosis_classification_system, diagnosis_basis, disease_phase,  sample_file_filters, survival_filters, treatment_filters, treatment_response_filters
where study_acronym in [''] and study_name in ['']
with id, participant_id, sample_id, diagnosis_id, dbgap_accession, sex_at_birth, race, age_at_diagnosis, diagnosis, diagnosis_anatomic_site, diagnosis_anatomic_site_str, diagnosis_classification_system, diagnosis_basis, disease_phase, sample_file_filters, survival_filters, treatment_filters, treatment_response_filters
where participant_id in [''] and sex_at_birth in [''] and ANY(element IN [''] WHERE element IN race)
with id, participant_id, sample_id, diagnosis_id, dbgap_accession, age_at_diagnosis, diagnosis, diagnosis_anatomic_site, diagnosis_anatomic_site_str, diagnosis_classification_system, diagnosis_basis, disease_phase, sample_file_filters, survival_filters, treatment_filters, treatment_response_filters
where age_at_diagnosis >= [''] and age_at_diagnosis <= [''] and diagnosis in [''] and ANY(element IN [''] WHERE element IN diagnosis_anatomic_site) and diagnosis_classification_system in [''] and diagnosis_basis in [''] and disease_phase in ['']
with id, participant_id, sample_id, diagnosis_id, dbgap_accession, age_at_diagnosis, diagnosis, diagnosis_anatomic_site, diagnosis_anatomic_site_str, diagnosis_classification_system, diagnosis_basis, disease_phase, sample_file_filters, survival_filters, treatment_filters, treatment_response_filters
unwind sample_file_filters as sample_file_filter
unwind survival_filters as survival_filter
unwind treatment_filters as treatment_filter
unwind treatment_response_filters as treatment_response_filter
with id, participant_id, sample_id, diagnosis_id, dbgap_accession, age_at_diagnosis, diagnosis, diagnosis_anatomic_site, diagnosis_anatomic_site_str, diagnosis_classification_system, diagnosis_basis, disease_phase, sample_file_filter, survival_filter, treatment_filter, treatment_response_filter
where sample_file_filter.participant_age_at_collection >= [''] and sample_file_filter.participant_age_at_collection <= [''] and ANY(element IN [''] WHERE element IN sample_file_filter.sample_anatomic_site) and sample_file_filter.sample_tumor_status in [''] and sample_file_filter.tumor_classification in [''] 
      and ANY(element IN [''] WHERE element IN sample_file_filter.data_category) and sample_file_filter.file_type in [''] and sample_file_filter.file_mapping_level in ['']
      and sample_file_filter.library_selection in [''] and sample_file_filter.library_source_material in [''] and sample_file_filter.library_source_molecule in [''] and sample_file_filter.library_strategy in [''] 
      and survival_filter.last_known_survival_status in [''] and survival_filter.event_free_survival_status in [''] and survival_filter.first_event in ['']
      and survival_filter.age_at_last_known_survival_status >= [''] and survival_filter.age_at_last_known_survival_status <= ['']
      and ANY(element IN [''] WHERE element IN treatment_filter.treatment_type) and ANY(element IN [''] WHERE element IN treatment_filter.treatment_agent)
      and treatment_filter.age_at_treatment_start >= [''] and treatment_filter.age_at_treatment_start <= ['']
      and treatment_response_filter.response_category in [''] and treatment_response_filter.age_at_response >= [''] and treatment_response_filter.age_at_response <= [''] 
with id, participant_id, sample_id, dbgap_accession, age_at_diagnosis, diagnosis, diagnosis_anatomic_site, diagnosis_anatomic_site_str, diagnosis_classification_system, diagnosis_basis, disease_phase
with distinct id, participant_id, sample_id, dbgap_accession, age_at_diagnosis, diagnosis, diagnosis_anatomic_site, diagnosis_anatomic_site_str, diagnosis_classification_system, diagnosis_basis, disease_phase
return
coalesce(participant_id, '') as `Participant ID`,
coalesce(sample_id, '') as `Sample ID`,
coalesce(dbgap_accession, '') as `Study ID`,
coalesce(diagnosis, '') as `Diagnosis`,
coalesce(diagnosis_anatomic_site_str, '') as `Diagnosis Anatomic Site`,
coalesce(diagnosis_classification_system, '') as `Diagnosis Classification System`,
coalesce(diagnosis_basis, '') as `Diagnosis Basis`,
coalesce(disease_phase, '') as `Disease Phase`,
case age_at_diagnosis when -999 then 'Not Reported' else coalesce(age_at_diagnosis, '') end as `Age at diagnosis (days)`
Order by participant_id limit 100
MATCH (p:participant)-->(st:study)
where st.dbgap_accession in ['']
optional match (p)<--(sm:sample)
optional match (p)<--(file)
where (file: clinical_measure_file or file: generic_file or file: radiology_file)
with distinct p, sm, file
with p, collect(DISTINCT {
            sample_anatomic_site: apoc.text.split(sm.anatomic_site, ';'),
            participant_age_at_collection: sm.participant_age_at_collection,
            sample_tumor_status: sm.sample_tumor_status,
            tumor_classification: sm.tumor_classification,
            data_category: apoc.text.split(file.data_category, ';'),
            file_type: file.file_type,
            file_mapping_level: file.file_mapping_level,
            library_source_material: null,
            library_source_molecule: null,
            library_strategy: null
    }) as sample_clinical_radiology_file_filter
optional match (p)<--(sm:sample)<--(file)
where (file: sequencing_file or file: generic_file or file: methylation_array_file or file: pathology_file or file: cytogenomic_file)
with p, sample_clinical_radiology_file_filter, collect(DISTINCT {
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
    }) as sample_sequencing_cytogenomic_pathology_methylation_file_filter
  with p, apoc.coll.union(sample_clinical_radiology_file_filter, sample_sequencing_cytogenomic_pathology_methylation_file_filter) as sample_file_filters
  optional match (p)<--(dg:diagnosis)
  with p, sample_file_filters, dg
  unwind sample_file_filters as sample_file_filter
  with p, collect(apoc.map.merge(sample_file_filter, {
      age_at_diagnosis: dg.age_at_diagnosis,
      diagnosis_anatomic_site: apoc.text.split(dg.anatomic_site, ';'),
      disease_phase: dg.disease_phase,
      diagnosis_classification_system: dg.diagnosis_classification_system,
      diagnosis_basis: dg.diagnosis_basis, 
      tumor_grade_source: dg.tumor_grade_source,
      tumor_stage_source: dg.tumor_stage_source,          
      diagnosis: dg.diagnosis
    })) as sample_diagnosis_file_filter
  optional match (p)<--(sm:sample)<--(dg:diagnosis)
  optional match (sm)<--(file)
  where (file: sequencing_file or file: generic_file or file: methylation_array_file or file: pathology_file or file: cytogenomic_file)
  with p, sample_diagnosis_file_filter, COLLECT(DISTINCT {
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
          diagnosis: dg.diagnosis,
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
      }) AS sample_diagnosis_filters_1
  with p, apoc.coll.union(sample_diagnosis_file_filter, sample_diagnosis_filters_1) as sample_diagnosis_file_filters
  optional match (p)<--(sm:sample)<--(dg:diagnosis)
  optional match (p)<--(file)
  where (file: clinical_measure_file or file: generic_file or file: radiology_file)
  with p, sample_diagnosis_file_filters, COLLECT(DISTINCT {
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
          diagnosis: dg.diagnosis,
          data_category: apoc.text.split(file.data_category, ';'),
          file_type: file.file_type,
          file_mapping_level: file.file_mapping_level,
          library_selection: null,
          library_source_material: null,
          library_source_molecule: null,
          library_strategy: null
      }) AS sample_diagnosis_filters_2
  with p, apoc.coll.union(sample_diagnosis_file_filters, sample_diagnosis_filters_2) as sample_diagnosis_file_filter
optional MATCH (p)<-[:of_sample]-(sm1:sample)<--(cl)<--(sm2:sample)
WHERE (cl: cell_line or cl: pdx)
optional Match (sm2)<--(file)
WHERE (file: sequencing_file or file: generic_file OR file:pathology_file OR file:methylation_array_file OR file:cytogenomic_file)
optional Match (sm1)<--(dg:diagnosis)
with p, sample_diagnosis_file_filter, COLLECT(DISTINCT {
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
          diagnosis: dg.diagnosis,
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
      }) AS sample_diagnosis_filters_1
  with p, apoc.coll.union(sample_diagnosis_file_filter, sample_diagnosis_filters_1) as sample_diagnosis_file_filters
optional MATCH (p)<-[:of_sample]-(sm1:sample)<--(cl)<--(sm2:sample)
WHERE (cl: cell_line or cl: pdx)
optional Match (sm2)<--(file)
WHERE (file: sequencing_file or file: generic_file OR file:pathology_file OR file:methylation_array_file OR file:cytogenomic_file)
optional Match (sm2)<--(dg:diagnosis)
with p, sample_diagnosis_file_filters, COLLECT(DISTINCT {
          sample_anatomic_site: apoc.text.split(sm2.anatomic_site, ';'),
          participant_age_at_collection: sm2.participant_age_at_collection,
          sample_tumor_status: sm2.sample_tumor_status,
          tumor_classification: sm2.tumor_classification,
          age_at_diagnosis: dg.age_at_diagnosis,
          diagnosis_anatomic_site: apoc.text.split(dg.anatomic_site, ';'),
          disease_phase: dg.disease_phase,
          diagnosis_classification_system: dg.diagnosis_classification_system,
          diagnosis_basis: dg.diagnosis_basis, 
          tumor_grade_source: dg.tumor_grade_source,
          tumor_stage_source: dg.tumor_stage_source,          
          diagnosis: dg.diagnosis,
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
      }) AS sample_diagnosis_filters_2
  with p, apoc.coll.union(sample_diagnosis_file_filters, sample_diagnosis_filters_2) as sample_diagnosis_file_filter
optional MATCH (p)<-[:of_sample]-(sm1:sample)<--(cl)<--(sm2:sample)
WHERE (cl: cell_line or cl: pdx)
optional Match (sm1)<--(dg:diagnosis)
optional match (p)<--(file)
where (file: clinical_measure_file or file: generic_file or file: radiology_file)
with p, sample_diagnosis_file_filter, COLLECT(DISTINCT {
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
          diagnosis: dg.diagnosis,
          data_category: apoc.text.split(file.data_category, ';'),
          file_type: file.file_type,
          file_mapping_level: file.file_mapping_level,
          library_selection: null,
          library_source_material: null,
          library_source_molecule: null,
          library_strategy: null
      }) AS sample_diagnosis_filters_3
  with p, apoc.coll.union(sample_diagnosis_file_filter, sample_diagnosis_filters_3) as sample_diagnosis_file_filters
optional MATCH (p)<-[:of_sample]-(sm1:sample)<--(cl)<--(sm2:sample)
WHERE (cl: cell_line or cl: pdx)
optional Match (sm2)<--(dg:diagnosis)
optional match (p)<--(file)
where (file: clinical_measure_file or file: generic_file or file: radiology_file)
with p, sample_diagnosis_file_filters, COLLECT(DISTINCT {
          sample_anatomic_site: apoc.text.split(sm2.anatomic_site, ';'),
          participant_age_at_collection: sm2.participant_age_at_collection,
          sample_tumor_status: sm2.sample_tumor_status,
          tumor_classification: sm2.tumor_classification,
          age_at_diagnosis: dg.age_at_diagnosis,
          diagnosis_anatomic_site: apoc.text.split(dg.anatomic_site, ';'),
          disease_phase: dg.disease_phase,
          diagnosis_classification_system: dg.diagnosis_classification_system,
          diagnosis_basis: dg.diagnosis_basis, 
          tumor_grade_source: dg.tumor_grade_source,
          tumor_stage_source: dg.tumor_stage_source,          
          diagnosis: dg.diagnosis,
          data_category: apoc.text.split(file.data_category, ';'),
          file_type: file.file_type,
          file_mapping_level: file.file_mapping_level,
          library_selection: null,
          library_source_material: null,
          library_source_molecule: null,
          library_strategy: null
      }) AS sample_diagnosis_filters_4
  with p, apoc.coll.union(sample_diagnosis_file_filters, sample_diagnosis_filters_4) as sample_diagnosis_file_filter
  OPTIONAL MATCH (p)<-[*..4]-(file)
  WHERE (file:clinical_measure_file or file: generic_file OR file: sequencing_file OR file:pathology_file OR file:radiology_file OR file:methylation_array_file OR file:cytogenomic_file)
  OPTIONAL MATCH (p)<-[:of_survival]-(su:survival)
  OPTIONAL MATCH (p)<-[:of_treatment]-(tm:treatment)
  OPTIONAL MATCH (p)<-[:of_treatment_response]-(tr:treatment_response)
  OPTIONAL MATCH (p)<-[:of_synonym]-(sy:synonym)
  OPTIONAL MATCH (st:study)<-[:of_participant]-(p)
  OPTIONAL MATCH (st)<-[:of_study_personnel]-(stp:study_personnel)
  OPTIONAL MATCH (st)<-[:of_study_funding]-(stf:study_funding)
  OPTIONAL MATCH (d:diagnosis)-[:of_diagnosis]->(p)
  WITH p, sy, sample_diagnosis_file_filter, COLLECT(DISTINCT {last_known_survival_status: su.last_known_survival_status, 
              event_free_survival_status: su.event_free_survival_status, 
              first_event: su.first_event,
              age_at_last_known_survival_status: su.age_at_last_known_survival_status} ) AS survival_filters,
            COLLECT(DISTINCT{treatment_type: apoc.text.split(tm.treatment_type, ';'),
            treatment_agent: apoc.text.split(tm.treatment_agent, ';'),
            age_at_treatment_start: tm.age_at_treatment_start}) as treatment_filters,
            COLLECT(DISTINCT{response_category: tr.response_category,
            age_at_response: tr.age_at_response}) as treatment_response_filters , file, st, stf, stp,
            apoc.text.join(COLLECT(distinct d.diagnosis), ';') AS diagnosis_str,
            apoc.text.join(apoc.coll.toSet(apoc.text.split(apoc.text.join(COLLECT(distinct d.anatomic_site), ';'), ';')), ';') AS diagnosis_anatomic_site_str,
            apoc.text.join(COLLECT(distinct toString(case d.age_at_diagnosis when -999 then 'Not Reported' else coalesce(d.age_at_diagnosis, '') end)), ';') AS age_at_diagnosis_str,
            apoc.text.join(apoc.coll.toSet(apoc.text.split(apoc.text.join(COLLECT(distinct tm.treatment_agent), ';'), ';')), ';') as treatment_agent_str,
            apoc.text.join(apoc.coll.toSet(apoc.text.split(apoc.text.join(COLLECT(distinct tm.treatment_type), ';'), ';')), ';') as treatment_type_str,
            apoc.text.join(COLLECT(distinct toString(case tm.age_at_treatment_start when -999 then 'Not Reported' else coalesce(tm.age_at_treatment_start, '') end)), ';') AS age_at_treatment_start_str,
            apoc.text.join(COLLECT(distinct su.first_event), ';') AS first_event_str,
            apoc.text.join(COLLECT(distinct su.last_known_survival_status), ';') AS last_known_survival_status_str,
            apoc.text.join(COLLECT(distinct toString(case su.age_at_last_known_survival_status when -999 then 'Not Reported' else coalesce(su.age_at_last_known_survival_status, '') end)), ';') AS age_at_last_known_survival_status_str
  with DISTINCT
    p.id as id,
    p.participant_id as participant_id,
    apoc.text.split(p.race, ';') as race,
    p.race as race_str,
    p.sex_at_birth as sex_at_birth,
    diagnosis_str as diagnosis_str,
    diagnosis_anatomic_site_str as diagnosis_anatomic_site_str,
    age_at_diagnosis_str as age_at_diagnosis_str,
    treatment_agent_str as treatment_agent_str,
    treatment_type_str as treatment_type_str,
    age_at_treatment_start_str as age_at_treatment_start_str,
    first_event_str as first_event_str,
    last_known_survival_status_str as last_known_survival_status_str,
    age_at_last_known_survival_status_str as age_at_last_known_survival_status_str,
    treatment_filters as treatment_filters,
    survival_filters as survival_filters,
    treatment_response_filters as treatment_response_filters,
    sample_diagnosis_file_filter AS sample_diagnosis_file_filters,
    st.study_id as study_id,
    st.dbgap_accession as dbgap_accession,
    st.study_acronym as study_acronym,
    st.study_name as study_name,
    st.study_status as study_status
  where study_acronym in [''] and study_name in [''] and study_status in ['']
  with id, participant_id, dbgap_accession, sex_at_birth, race_str, race, diagnosis_str, diagnosis_anatomic_site_str, age_at_diagnosis_str, treatment_agent_str, treatment_type_str, age_at_treatment_start_str, first_event_str, last_known_survival_status_str, age_at_last_known_survival_status_str, sample_diagnosis_file_filters, survival_filters, treatment_filters, treatment_response_filters
  where participant_id in [''] and sex_at_birth in [''] and ANY(element IN [''] WHERE element IN race)
  unwind sample_diagnosis_file_filters as sample_diagnosis_file_filter
  unwind survival_filters as survival_filter
  unwind treatment_filters as treatment_filter
  unwind treatment_response_filters as treatment_response_filter
  with id, participant_id, dbgap_accession, sex_at_birth, race_str, diagnosis_str, diagnosis_anatomic_site_str, age_at_diagnosis_str, treatment_agent_str, treatment_type_str, age_at_treatment_start_str, first_event_str, last_known_survival_status_str, age_at_last_known_survival_status_str, sample_diagnosis_file_filter, survival_filter, treatment_filter, treatment_response_filter
  where sample_diagnosis_file_filter.age_at_diagnosis >= [''] and sample_diagnosis_file_filter.age_at_diagnosis <= [''] and sample_diagnosis_file_filter.diagnosis in [''] and ANY(element IN [''] WHERE element IN sample_diagnosis_file_filter.diagnosis_anatomic_site) and sample_diagnosis_file_filter.diagnosis_classification_system in [''] and sample_diagnosis_file_filter.diagnosis_basis in [''] and sample_diagnosis_file_filter.disease_phase in [''] 
        and sample_diagnosis_file_filter.participant_age_at_collection >= [''] and sample_diagnosis_file_filter.participant_age_at_collection <= [''] and ANY(element IN [''] WHERE element IN sample_diagnosis_file_filter.sample_anatomic_site) and sample_diagnosis_file_filter.sample_tumor_status in [''] and sample_diagnosis_file_filter.tumor_classification in [''] 
        and ANY(element IN [''] WHERE element IN sample_diagnosis_file_filter.data_category) and sample_diagnosis_file_filter.file_type in [''] and sample_diagnosis_file_filter.file_mapping_level in ['']
        and sample_diagnosis_file_filter.library_selection in [''] and sample_diagnosis_file_filter.library_source_material in [''] and sample_diagnosis_file_filter.library_source_molecule in [''] and sample_diagnosis_file_filter.library_strategy in ['']
        and survival_filter.last_known_survival_status in [''] and survival_filter.event_free_survival_status in [''] and survival_filter.first_event in ['']
        and survival_filter.age_at_last_known_survival_status >= [''] and survival_filter.age_at_last_known_survival_status <= ['']
        and ANY(element IN [''] WHERE element IN treatment_filter.treatment_type) and ANY(element IN [''] WHERE element IN treatment_filter.treatment_agent)
        and treatment_filter.age_at_treatment_start >= [''] and treatment_filter.age_at_treatment_start <= ['']
        and treatment_response_filter.response_category in [''] and treatment_response_filter.age_at_response >= [''] and treatment_response_filter.age_at_response <= [''] 
with distinct id, participant_id, dbgap_accession, sex_at_birth, race_str, diagnosis_str, diagnosis_anatomic_site_str, age_at_diagnosis_str, treatment_agent_str, treatment_type_str, age_at_treatment_start_str, first_event_str, last_known_survival_status_str, age_at_last_known_survival_status_str
  return
  coalesce(participant_id, '') AS `Participant ID`,
  coalesce(dbgap_accession, '') AS `Study ID`,
  coalesce(sex_at_birth, '') AS `Sex at Birth` ,
  coalesce(race_str, '') AS `Race`,
  diagnosis_str as `Diagnosis`,
  diagnosis_anatomic_site_str as `Diagnosis Anatomic Site`,
  age_at_diagnosis_str as `Age at Diagnosis (days)`,
  treatment_agent_str as `Treatment Agent`,
  treatment_type_str as `Treatment Type`,
  age_at_treatment_start_str as `Age at Treatment Start (days)`,
  first_event_str as `First Event`,
  last_known_survival_status_str as `Last Known Survival Status`,
  age_at_last_known_survival_status_str as `Age at Last Known Survival Status (days)`
  Order by participant_id Limit 100
MATCH (p:participant)-->(st:study)
where st.dbgap_accession in ['']
optional match (p)<--(sm:sample)
optional match (p)<--(file)
where (file: clinical_measure_file or file: radiology_file)
with distinct p, sm, file
with p, collect(DISTINCT {
            sample_anatomic_site: sm.anatomic_site,
            participant_age_at_collection: sm.participant_age_at_collection,
            sample_tumor_status: sm.sample_tumor_status,
            tumor_classification: sm.tumor_classification,
            assay_method: CASE labels(file)[0] WHEN 'clinical_measure_file' THEN 'Clinical data'
                              WHEN 'radiology_file' THEN 'Radiology imaging'
                              ELSE null END,
            file_type: file.file_type,
            library_source_material: null,
            library_source_molecule: null,
            library_strategy: null
    }) as sample_clinical_radiology_file_filter
optional match (p)<--(sm:sample)<--(file)
where (file: sequencing_file or file: methylation_array_file or file: pathology_file or file: cytogenomic_file)
with p, sample_clinical_radiology_file_filter, collect(DISTINCT {
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
    }) as sample_sequencing_cytogenomic_pathology_methylation_file_filter
  with p, apoc.coll.union(sample_clinical_radiology_file_filter, sample_sequencing_cytogenomic_pathology_methylation_file_filter) as sample_file_filters
  optional match (p)<--(dg:diagnosis)
  with p, sample_file_filters, dg
  unwind sample_file_filters as sample_file_filter
  with p, collect(apoc.map.merge(sample_file_filter, {
      age_at_diagnosis: dg.age_at_diagnosis,
      diagnosis_anatomic_site: dg.anatomic_site,
      disease_phase: dg.disease_phase,
      diagnosis_classification_system: dg.diagnosis_classification_system,
      diagnosis_basis: dg.diagnosis_basis, 
      tumor_grade_source: dg.tumor_grade_source,
      tumor_stage_source: dg.tumor_stage_source,          
      diagnosis: dg.diagnosis
    })) as sample_diagnosis_file_filter
  optional match (p)<--(sm:sample)<--(dg:diagnosis)
  optional match (sm)<--(file)
  where (file: sequencing_file or file: methylation_array_file or file: pathology_file or file: cytogenomic_file)
  with p, sample_diagnosis_file_filter, COLLECT(DISTINCT {
          sample_anatomic_site: sm.anatomic_site,
          participant_age_at_collection: sm.participant_age_at_collection,
          sample_tumor_status: sm.sample_tumor_status,
          tumor_classification: sm.tumor_classification,
          age_at_diagnosis: dg.age_at_diagnosis,
          diagnosis_anatomic_site: dg.anatomic_site,
          disease_phase: dg.disease_phase,
          diagnosis_classification_system: dg.diagnosis_classification_system,
          diagnosis_basis: dg.diagnosis_basis, 
          tumor_grade_source: dg.tumor_grade_source,
          tumor_stage_source: dg.tumor_stage_source,          
          diagnosis: dg.diagnosis,
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
      }) AS sample_diagnosis_filters_1
  with p, apoc.coll.union(sample_diagnosis_file_filter, sample_diagnosis_filters_1) as sample_diagnosis_file_filters
  optional match (p)<--(sm:sample)<--(dg:diagnosis)
  optional match (p)<--(file)
  where (file: clinical_measure_file or file: radiology_file)
  with p, sample_diagnosis_file_filters, COLLECT(DISTINCT {
          sample_anatomic_site: sm.anatomic_site,
          participant_age_at_collection: sm.participant_age_at_collection,
          sample_tumor_status: sm.sample_tumor_status,
          tumor_classification: sm.tumor_classification,
          age_at_diagnosis: dg.age_at_diagnosis,
          diagnosis_anatomic_site: dg.anatomic_site,
          disease_phase: dg.disease_phase,
          diagnosis_classification_system: dg.diagnosis_classification_system,
          diagnosis_basis: dg.diagnosis_basis, 
          tumor_grade_source: dg.tumor_grade_source,
          tumor_stage_source: dg.tumor_stage_source,          
          diagnosis: dg.diagnosis,
          assay_method: CASE labels(file)[0] WHEN 'clinical_measure_file' THEN 'Clinical data'
                              WHEN 'radiology_file' THEN 'Radiology imaging'
                              ELSE null END,
          file_type: file.file_type,
          library_selection: null,
          library_source_material: null,
          library_source_molecule: null,
          library_strategy: null
      }) AS sample_diagnosis_filters_2
  with p, apoc.coll.union(sample_diagnosis_file_filters, sample_diagnosis_filters_2) as sample_diagnosis_file_filter
optional MATCH (p)<-[:of_sample]-(sm1:sample)<--(cl)<--(sm2:sample)
WHERE (cl: cell_line or cl: pdx)
optional Match (sm2)<--(file)
WHERE (file: sequencing_file OR file:pathology_file OR file:methylation_array_file OR file:cytogenomic_file)
optional Match (sm1)<--(dg:diagnosis)
with p, sample_diagnosis_file_filter, COLLECT(DISTINCT {
          sample_anatomic_site: sm1.anatomic_site,
          participant_age_at_collection: sm1.participant_age_at_collection,
          sample_tumor_status: sm1.sample_tumor_status,
          tumor_classification: sm1.tumor_classification,
          age_at_diagnosis: dg.age_at_diagnosis,
          diagnosis_anatomic_site: dg.anatomic_site,
          disease_phase: dg.disease_phase,
          diagnosis_classification_system: dg.diagnosis_classification_system,
          diagnosis_basis: dg.diagnosis_basis, 
          tumor_grade_source: dg.tumor_grade_source,
          tumor_stage_source: dg.tumor_stage_source,          
          diagnosis: dg.diagnosis,
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
      }) AS sample_diagnosis_filters_1
  with p, apoc.coll.union(sample_diagnosis_file_filter, sample_diagnosis_filters_1) as sample_diagnosis_file_filters
optional MATCH (p)<-[:of_sample]-(sm1:sample)<--(cl)<--(sm2:sample)
WHERE (cl: cell_line or cl: pdx)
optional Match (sm2)<--(file)
WHERE (file: sequencing_file OR file:pathology_file OR file:methylation_array_file OR file:cytogenomic_file)
optional Match (sm2)<--(dg:diagnosis)
with p, sample_diagnosis_file_filters, COLLECT(DISTINCT {
          sample_anatomic_site: sm2.anatomic_site,
          participant_age_at_collection: sm2.participant_age_at_collection,
          sample_tumor_status: sm2.sample_tumor_status,
          tumor_classification: sm2.tumor_classification,
          age_at_diagnosis: dg.age_at_diagnosis,
          diagnosis_anatomic_site: dg.anatomic_site,
          disease_phase: dg.disease_phase,
          diagnosis_classification_system: dg.diagnosis_classification_system,
          diagnosis_basis: dg.diagnosis_basis, 
          tumor_grade_source: dg.tumor_grade_source,
          tumor_stage_source: dg.tumor_stage_source,          
          diagnosis: dg.diagnosis,
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
      }) AS sample_diagnosis_filters_2
  with p, apoc.coll.union(sample_diagnosis_file_filters, sample_diagnosis_filters_2) as sample_diagnosis_file_filter
optional MATCH (p)<-[:of_sample]-(sm1:sample)<--(cl)<--(sm2:sample)
WHERE (cl: cell_line or cl: pdx)
optional Match (sm1)<--(dg:diagnosis)
optional match (p)<--(file)
where (file: clinical_measure_file or file: radiology_file)
with p, sample_diagnosis_file_filter, COLLECT(DISTINCT {
          sample_anatomic_site: sm1.anatomic_site,
          participant_age_at_collection: sm1.participant_age_at_collection,
          sample_tumor_status: sm1.sample_tumor_status,
          tumor_classification: sm1.tumor_classification,
          age_at_diagnosis: dg.age_at_diagnosis,
          diagnosis_anatomic_site: dg.anatomic_site,
          disease_phase: dg.disease_phase,
          diagnosis_classification_system: dg.diagnosis_classification_system,
          diagnosis_basis: dg.diagnosis_basis, 
          tumor_grade_source: dg.tumor_grade_source,
          tumor_stage_source: dg.tumor_stage_source,          
          diagnosis: dg.diagnosis,
          assay_method: CASE labels(file)[0] WHEN 'clinical_measure_file' THEN 'Clinical data'
                              WHEN 'radiology_file' THEN 'Radiology imaging'
                              ELSE null END,
          file_type: file.file_type,
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
where (file: clinical_measure_file or file: radiology_file)
with p, sample_diagnosis_file_filters, COLLECT(DISTINCT {
          sample_anatomic_site: sm2.anatomic_site,
          participant_age_at_collection: sm2.participant_age_at_collection,
          sample_tumor_status: sm2.sample_tumor_status,
          tumor_classification: sm2.tumor_classification,
          age_at_diagnosis: dg.age_at_diagnosis,
          diagnosis_anatomic_site: dg.anatomic_site,
          disease_phase: dg.disease_phase,
          diagnosis_classification_system: dg.diagnosis_classification_system,
          diagnosis_basis: dg.diagnosis_basis, 
          tumor_grade_source: dg.tumor_grade_source,
          tumor_stage_source: dg.tumor_stage_source,          
          diagnosis: dg.diagnosis,
          assay_method: CASE labels(file)[0] WHEN 'clinical_measure_file' THEN 'Clinical data'
                              WHEN 'radiology_file' THEN 'Radiology imaging'
                              ELSE null END,
          file_type: file.file_type,
          library_selection: null,
          library_source_material: null,
          library_source_molecule: null,
          library_strategy: null
      }) AS sample_diagnosis_filters_4
  with p, apoc.coll.union(sample_diagnosis_file_filters, sample_diagnosis_filters_4) as sample_diagnosis_file_filter
  OPTIONAL MATCH (p)<-[*..4]-(file)
  WHERE (file:clinical_measure_file OR file: sequencing_file OR file:pathology_file OR file:radiology_file OR file:methylation_array_file OR file:cytogenomic_file)
  OPTIONAL MATCH (p)<-[:of_survival]-(su:survival)
  OPTIONAL MATCH (p)<-[:of_synonym]-(sy:synonym)
  OPTIONAL MATCH (st:study)<-[:of_participant]-(p)
  OPTIONAL MATCH (st)<-[:of_study_personnel]-(stp:study_personnel)
  OPTIONAL MATCH (st)<-[:of_study_funding]-(stf:study_funding)
  WITH p, sy, sample_diagnosis_file_filter, COLLECT(DISTINCT su.last_known_survival_status) as vital_status, file, st, stf, stp
  with DISTINCT
    p.id as id,
    p.participant_id as participant_id,
    apoc.text.split(p.race, ';') as race,
    p.race as race_str,
    p.sex_at_birth as sex_at_birth,
    apoc.text.join(Collect(distinct sy.synonym_id), ',') as alternate_participant_id,
    case when 'Dead' in vital_status then ['Dead']
          else vital_status end as last_known_survival_status,
    sample_diagnosis_file_filter AS sample_diagnosis_file_filters,
    st.study_id as study_id,
    st.dbgap_accession as dbgap_accession,
    COLLECT(DISTINCT stf.grant_id) as grant_id,
    COLLECT(DISTINCT stp.institution) as institution,
    st.study_acronym as study_acronym,
    st.study_name as study_name
  where study_acronym in [''] and study_name in ['']
  with id, participant_id, dbgap_accession, sex_at_birth, race_str, race, alternate_participant_id, sample_diagnosis_file_filters, last_known_survival_status
  where participant_id in [''] and sex_at_birth in [''] and ANY(element IN [''] WHERE element IN race)
  unwind sample_diagnosis_file_filters as sample_diagnosis_file_filter
  with id, participant_id, dbgap_accession, sex_at_birth, race_str, alternate_participant_id, sample_diagnosis_file_filter, last_known_survival_status
  where sample_diagnosis_file_filter.age_at_diagnosis >= [''] and sample_diagnosis_file_filter.age_at_diagnosis <= [''] and sample_diagnosis_file_filter.diagnosis in [''] and sample_diagnosis_file_filter.diagnosis_anatomic_site in [''] and sample_diagnosis_file_filter.diagnosis_classification_system in [''] and sample_diagnosis_file_filter.diagnosis_basis in [''] and sample_diagnosis_file_filter.disease_phase in [''] 
        and sample_diagnosis_file_filter.participant_age_at_collection >= [''] and sample_diagnosis_file_filter.participant_age_at_collection <= [''] and sample_diagnosis_file_filter.sample_anatomic_site in [''] and sample_diagnosis_file_filter.sample_tumor_status in [''] and sample_diagnosis_file_filter.tumor_classification in [''] 
        and sample_diagnosis_file_filter.assay_method in [''] and sample_diagnosis_file_filter.file_type in [''] 
        and sample_diagnosis_file_filter.library_selection in [''] and sample_diagnosis_file_filter.library_source_material in [''] and sample_diagnosis_file_filter.library_source_molecule in [''] and sample_diagnosis_file_filter.library_strategy in ['']
  with id, participant_id, dbgap_accession, sex_at_birth, race_str, alternate_participant_id, last_known_survival_status
  where ANY(element IN [''] WHERE element IN last_known_survival_status)
  with distinct id, participant_id, dbgap_accession, sex_at_birth, race_str, alternate_participant_id
  return
  coalesce(participant_id, '') AS `Participant ID`,
  coalesce(dbgap_accession, '') AS `Study ID`,
  coalesce(sex_at_birth, '') AS `Sex` ,
  coalesce(race_str, '') AS `Race`,
  coalesce(alternate_participant_id, '') AS `Synonym Participant ID`
  Order by participant_id Limit 100
MATCH (st:study)
where st.dbgap_accession in ['']
with st
call {
  with st
  MATCH (sm:sample)-[*..4]->(st)
  WITH sm, {
    id: sm.id,
    sample_id: sm.sample_id,
    sample_anatomic_site: apoc.text.split(sm.anatomic_site, ';'),
    sample_anatomic_site_str: sm.anatomic_site,
    participant_age_at_collection: sm.participant_age_at_collection,
    sample_tumor_status: sm.sample_tumor_status,
    tumor_classification: sm.tumor_classification
  } AS opensearch_data
  MATCH (sm)-[*..3]->(p:participant)
  WITH sm, apoc.map.merge(opensearch_data, {
    pid: p.id,
    participant_id: p.participant_id,
    race: apoc.text.split(p.race, ';'),
    sex_at_birth: p.sex_at_birth
  }) AS opensearch_data
  OPTIONAL MATCH (sm)-[*..3]->(:participant)<--(dg:diagnosis)
  WITH sm, opensearch_data, COLLECT(DISTINCT {
    age_at_diagnosis: dg.age_at_diagnosis,
    diagnosis_anatomic_site: apoc.text.split(dg.anatomic_site, ';'),
    disease_phase: dg.disease_phase,
    diagnosis_classification_system: dg.diagnosis_classification_system,
    diagnosis_basis: dg.diagnosis_basis,
    tumor_grade_source: dg.tumor_grade_source,
    tumor_stage_source: dg.tumor_stage_source,
    diagnosis: dg.diagnosis
  }) AS diagnosis_filter
  OPTIONAL MATCH (sm)<-[*..3]-(dg:diagnosis)
  WITH sm, apoc.map.merge(opensearch_data, {
    diagnosis_filters: apoc.coll.union(diagnosis_filter, COLLECT(DISTINCT{
      age_at_diagnosis: dg.age_at_diagnosis,
      diagnosis_anatomic_site: apoc.text.split(dg.anatomic_site, ';'),
      disease_phase: dg.disease_phase,
      diagnosis_classification_system: dg.diagnosis_classification_system,
      diagnosis_basis: dg.diagnosis_basis,
      tumor_grade_source: dg.tumor_grade_source,
      tumor_stage_source: dg.tumor_stage_source,
      diagnosis: dg.diagnosis
    }))
  }) AS opensearch_data
  OPTIONAL MATCH (sm)<-[*..3]-(file:sequencing_file)
  WITH sm, opensearch_data, COLLECT(DISTINCT {
    assay_method: 'Sequencing',
    file_type: file.file_type,
    library_selection: file.library_selection,
    library_source_material: file.library_source_material,
    library_source_molecule: file.library_source_molecule,
    library_strategy: file.library_strategy
  }) AS file_filter
  OPTIONAL MATCH (sm)<-[*..3]-(file:pathology_file)
  WITH sm, opensearch_data, apoc.coll.union(file_filter, COLLECT(DISTINCT {
    assay_method: 'Pathology imaging',
    file_type: file.file_type,
    library_selection: null,
    library_source_material: null,
    library_source_molecule: null,
    library_strategy: null
  })) AS file_filter
  OPTIONAL MATCH (sm)<-[*..3]-(file:cytogenomic_file)
  WITH sm, opensearch_data, apoc.coll.union(file_filter, COLLECT(DISTINCT {
    assay_method: 'Cytogenomic',
    file_type: file.file_type,
    library_selection: null,
    library_source_material: null,
    library_source_molecule: null,
    library_strategy: null
  })) AS file_filter
  OPTIONAL MATCH (sm)<-[*..3]-(file:methylation_array_file)
  WITH sm, opensearch_data, apoc.coll.union(file_filter, COLLECT(DISTINCT {
    assay_method: 'Methylation array',
    file_type: file.file_type,
    library_selection: null,
    library_source_material: null,
    library_source_molecule: null,
    library_strategy: null
  })) AS file_filter
  WITH sm, apoc.map.merge(opensearch_data, {
    file_filters: file_filter
  }) AS opensearch_data
  MATCH (sm)-[*..3]->(:participant)-[:of_participant]->(st:study)
  WITH sm, apoc.map.merge(opensearch_data, {
    study_id: st.study_id,
    dbgap_accession: st.dbgap_accession,
    study_acronym: st.study_acronym,
    study_name: st.study_name
  }) AS opensearch_data
  OPTIONAL MATCH (sm)-[*..3]->(:participant)<-[:of_survival]-(su:survival)
  WITH sm, opensearch_data, COLLECT(DISTINCT su.last_known_survival_status) as vital_status
  WITH sm, apoc.map.merge(opensearch_data, {
    last_known_survival_status: CASE 
        WHEN 'Dead' IN vital_status THEN ['Dead']
        ELSE vital_status 
      END
  }) AS opensearch_data
  with opensearch_data
  RETURN DISTINCT
    opensearch_data.id as id,
    opensearch_data.pid as pid,
    opensearch_data.sample_id as sample_id,
    opensearch_data.participant_id as participant_id,
    opensearch_data.race as race,
    opensearch_data.sex_at_birth as sex_at_birth,
    opensearch_data.sample_anatomic_site as sample_anatomic_site,
    opensearch_data.sample_anatomic_site_str as sample_anatomic_site_str,
    opensearch_data.participant_age_at_collection as participant_age_at_collection,
    opensearch_data.sample_tumor_status as sample_tumor_status,
    opensearch_data.tumor_classification as tumor_classification,
    opensearch_data.study_id as study_id,
    opensearch_data.dbgap_accession as dbgap_accession,
    opensearch_data.study_acronym as study_acronym,
    opensearch_data.study_name as study_name,
    opensearch_data.diagnosis_filters AS diagnosis_filters,
    opensearch_data.last_known_survival_status as last_known_survival_status,
    opensearch_data.file_filters AS file_filters
  union all
    with st
    MATCH (sm:sample)
    MATCH (st)<-[:of_cell_line|of_pdx]-(cl)<--(sm)
    Where (cl:cell_line or cl:pdx)
    optional Match (sm)<--(file)
    WHERE (file: sequencing_file OR file:pathology_file OR file:methylation_array_file OR file:cytogenomic_file)
    OPTIONAL MATCH (sm)<-[:of_diagnosis]-(dg:diagnosis)
    OPTIONAL MATCH (st)<-[:of_study_personnel]-(stp:study_personnel)
    OPTIONAL MATCH (st)<-[:of_study_funding]-(stf:study_funding)
    WITH sm, file, st, stf, stp, dg
    RETURN DISTINCT
      sm.id as id,
      null as pid,
      sm.sample_id as sample_id,
      null as participant_id,
      null as race,
      null as sex_at_birth,
      apoc.text.split(sm.anatomic_site, ';') as sample_anatomic_site,
      sm.anatomic_site as sample_anatomic_site_str,
      sm.participant_age_at_collection as participant_age_at_collection,
      sm.sample_tumor_status as sample_tumor_status,
      sm.tumor_classification as tumor_classification,
      st.study_id as study_id,
      st.dbgap_accession as dbgap_accession,
      st.study_acronym as study_acronym,
      st.study_name as study_name,
      COLLECT(DISTINCT {
          age_at_diagnosis: dg.age_at_diagnosis,
          diagnosis_anatomic_site: apoc.text.split(dg.anatomic_site, ';'),
          disease_phase: dg.disease_phase,
          diagnosis_classification_system: dg.diagnosis_classification_system,
          diagnosis_basis: dg.diagnosis_basis,
          tumor_grade_source: dg.tumor_grade_source,
          tumor_stage_source: dg.tumor_stage_source,
          diagnosis: dg.diagnosis
      }) AS diagnosis_filters,
      null as last_known_survival_status,
      CASE COLLECT(file) WHEN [] THEN []
                ELSE COLLECT(DISTINCT {
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
                }) END AS file_filters
}
with id, sample_id, participant_id, study_id, sex_at_birth, race, participant_age_at_collection, sample_anatomic_site, sample_anatomic_site_str, sample_tumor_status, tumor_classification, diagnosis_filters, last_known_survival_status, file_filters, dbgap_accession, study_acronym, study_name
where study_acronym in [''] and study_name in ['']
with id, sample_id, participant_id, study_id, sex_at_birth, race, participant_age_at_collection, sample_anatomic_site, sample_anatomic_site_str, sample_tumor_status, tumor_classification, diagnosis_filters, last_known_survival_status, file_filters, dbgap_accession
where participant_id in [''] and sex_at_birth in [''] and ANY(element IN [''] WHERE element IN race)
unwind diagnosis_filters as diagnosis_filter
with id, sample_id, participant_id, study_id, sex_at_birth, race, participant_age_at_collection, sample_anatomic_site, sample_anatomic_site_str, sample_tumor_status, tumor_classification, diagnosis_filter, last_known_survival_status, file_filters, dbgap_accession
where diagnosis_filter.age_at_diagnosis >= [''] and diagnosis_filter.age_at_diagnosis <= [''] and diagnosis_filter.diagnosis in [''] and ANY(element IN [''] WHERE element IN diagnosis_filter.diagnosis_anatomic_site) and diagnosis_filter.diagnosis_classification_system in [''] and diagnosis_filter.diagnosis_basis in [''] and diagnosis_filter.disease_phase in ['']
with id, sample_id, participant_id, study_id, sex_at_birth, race, participant_age_at_collection, sample_anatomic_site, sample_anatomic_site_str, sample_tumor_status, tumor_classification, last_known_survival_status, file_filters, dbgap_accession
where ANY(element IN [''] WHERE element IN last_known_survival_status) 
with id, sample_id, participant_id, study_id, sex_at_birth, race, participant_age_at_collection, sample_anatomic_site, sample_anatomic_site_str, sample_tumor_status, tumor_classification, file_filters, dbgap_accession
where participant_age_at_collection >= [''] and participant_age_at_collection <= [''] and ANY(element IN [''] WHERE element IN sample_anatomic_site) and sample_tumor_status in [''] and tumor_classification in ['']
unwind file_filters as file_filter
with id, sample_id, participant_id, study_id, sex_at_birth, race, participant_age_at_collection, sample_anatomic_site, sample_anatomic_site_str, sample_tumor_status, tumor_classification, file_filter, dbgap_accession
where file_filter.assay_method in [''] and file_filter.file_type in [''] 
      and file_filter.library_selection in [''] and file_filter.library_source_material in [''] and file_filter.library_source_molecule in [''] and file_filter.library_strategy in ['']
with distinct id, sample_id, participant_id, study_id, sex_at_birth, race, participant_age_at_collection, sample_anatomic_site, sample_anatomic_site_str, sample_tumor_status, tumor_classification, dbgap_accession
RETURN DISTINCT
  sample_id as `Sample ID`,
  participant_id as `Participant ID`,
  study_id as `Study ID`,
  sample_anatomic_site_str as `Anatomic Site`,
  case participant_age_at_collection when -999 then 'Not Reported' else coalesce(participant_age_at_collection, '') end as `Age at Sample Collection`,
  sample_tumor_status as `Sample Tumor Status`,
  tumor_classification as `Sample Tumor Classification`
Order by sample_id Limit 100


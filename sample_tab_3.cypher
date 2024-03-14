MATCH (st:study)<-[:of_participant]-(p:participant)
WHERE st.phs_accession IN ['phs003111']
        OPTIONAL MATCH (p)<-[*..4]-(file)
        WHERE (file:clinical_measure_file OR file: sequencing_file OR file:pathology_file OR file:radiology_file OR file:methylation_array_file OR file:single_cell_sequencing_file OR file:cytogenomic_file)
        optional MATCH (st)<--(file1:clinical_measure_file)
        MATCH (p)<-[:of_diagnosis]-(dg:diagnosis)
        where  dg.diagnosis_verification_status='Unknown' and dg.disease_phase='Progression'
        optional MATCH (p)<-[*..3]-(sm:sample)
  
WITH DISTINCT p, st, sm, dg
RETURN DISTINCT
  sm.sample_id AS `Sample ID`,
  p.participant_id AS `Participant ID`,
  st.study_id AS `Study ID`,
  sm.anatomic_site AS `Anatomic Site`,
  CASE sm.participant_age_at_collection WHEN -999 THEN 'Not Reported' ELSE COALESCE(sm.participant_age_at_collection, '') END AS `Age at Sample Collection`,
  COALESCE(sm.diagnosis_classification, '') AS `Diagnosis`,
  COALESCE(sm.diagnosis_classification_system, '') AS `Diagnosis Classification System`,
  COALESCE(sm.diagnosis_verification_status, '') AS `Diagnosis Verification Status`,
  COALESCE(sm.diagnosis_basis, '') AS `Diagnosis Basis`,
  COALESCE(sm.diagnosis_comment, '') AS `Diagnosis Comment`,
  sm.sample_tumor_status AS `Sample Tumor Status`,
  sm.tumor_classification AS `Sample Tumor Classification`
ORDER BY `Sample ID`
LIMIT 100
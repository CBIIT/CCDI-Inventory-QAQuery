MATCH (st:study)<-[:of_participant]-(p:participant)
WHERE st.phs_accession IN ['phs003111'] and p.race contains 'White'
optional MATCH (st)<--(file1:clinical_measure_file)
optional Match (p)<--(file2)
where (file2: clinical_measure_file OR file2:radiology_file)
        MATCH (p)<-[:of_diagnosis]-(dg:diagnosis)
        where  dg.diagnosis_classification_system = 'ICD-O-3.2'
        MATCH (p)<-[*..3]-(sm:sample)
        where sm.anatomic_site = 'C22.0 : Liver'
        OPTIONAL MATCH (sm)<--(file)
        WHERE (file: sequencing_file OR file:pathology_file OR file:methylation_array_file OR file:single_cell_sequencing_file OR file:cytogenomic_file)
        WITH file,file1,file2, p, st, sm, dg
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
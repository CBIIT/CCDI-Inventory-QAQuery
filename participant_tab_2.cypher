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
        return      coalesce(p.participant_id, '') AS `Participant ID`,
  coalesce(st.phs_accession, '') AS `Study ID`,
  coalesce(p.sex_at_birth, '') AS `Sex` ,
  coalesce(p.race, '') AS `Race`,
  coalesce(p.ethnicity, '') AS `Ethnicity` ,
  coalesce(p.alternate_participant_id, '') AS `Alternate ID`
Order by p.participant_id Limit 100
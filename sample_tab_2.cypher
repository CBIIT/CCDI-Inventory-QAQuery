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
        return    sm.sample_id as `Sample ID`,
          p.participant_id as `Participant ID`,
          st.study_id as `Study ID`,
          sm.anatomic_site as `Anatomic Site`,
          case sm.participant_age_at_collection when -999 then 'Not Reported' else coalesce(sm.participant_age_at_collection, '') end as `Age at Sample Collection`,
          coalesce(sm.diagnosis_classification, '') as `Diagnosis`,
          coalesce(sm.diagnosis_classification_system, '') as `Diagnosis Classification System`,
          coalesce(sm.diagnosis_verification_status, '') as `Diagnosis Verification Status`,
          coalesce(sm.diagnosis_basis, '') as `Diagnosis Basis`,
          coalesce(sm.diagnosis_comment, '') as `Diagnosis Comment`,
          sm.sample_tumor_status as `Sample Tumor Status`,
          sm.tumor_classification as `Sample Tumor Classification`
Order by sm.sample_id Limit 100
MATCH (st:study)<-[:of_participant]-(p:participant)
WHERE st.phs_accession IN ['phs003111']
        OPTIONAL MATCH (p)<-[*..4]-(file)
        WHERE (file:clinical_measure_file OR file: sequencing_file OR file:pathology_file OR file:radiology_file OR file:methylation_array_file OR file:single_cell_sequencing_file OR file:cytogenomic_file)
        optional MATCH (st)<--(file1:clinical_measure_file)
        MATCH (p)<-[:of_diagnosis]-(dg:diagnosis)
        where  dg.diagnosis_verification_status='Unknown' and dg.disease_phase='Progression'
        optional MATCH (p)<-[*..3]-(sm:sample)
  OPTIONAL MATCH(fo:follow_up)-->(p)
        WITH file, file1, p, st, sm, dg,fo
        return
        coalesce(p.participant_id, '') as `Participant ID`,
coalesce(st.phs_accession, '') as `Study ID`,
coalesce(dg.diagnosis_classification, '') as `Diagnosis`,
coalesce(dg.diagnosis_classification_system, '') as `Diagnosis Classification System`,
coalesce(dg.diagnosis_verification_status, '') as `Diagnosis Verification Status`,
coalesce(dg.diagnosis_basis, '') as `Diagnosis Basis`,
coalesce(dg.diagnosis_comment, '') as `Diagnosis Comment`,
coalesce(dg.disease_phase, '') as `Disease Phase`,
coalesce(dg.anatomic_site, '') as `Anatomic Site`,
case dg.age_at_diagnosis when -999 then 'Not Reported' else coalesce(dg.age_at_diagnosis, '') end as `Age at diagnosis (days)`,
coalesce(fo.vital_status, '') as `Vital Status`
Order by p.participant_id
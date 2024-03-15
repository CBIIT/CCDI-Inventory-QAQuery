MATCH (st:study)<-[:of_participant]-(p:participant)
WHERE st.phs_accession IN ['phs003111']
        OPTIONAL MATCH (p)<-[*..4]-(file)
        WHERE (file:clinical_measure_file OR file: sequencing_file OR file:pathology_file OR file:radiology_file OR file:methylation_array_file OR file:single_cell_sequencing_file OR file:cytogenomic_file)
        optional MATCH (st)<--(file1:clinical_measure_file)
        MATCH (p)<-[:of_diagnosis]-(dg:diagnosis)
        where  dg.diagnosis_verification_status='Unknown' and dg.disease_phase='Progression'
        optional MATCH (p)<-[*..3]-(sm:sample)
  
        WITH file, file1, p, st, sm, dg
        return
        count(distinct st.id) as Studies,
        count(distinct p.id)as Participants,
          count(distinct sm.id) as Samples,
          count(distinct dg.id) as Diagnosis,
        count(distinct file.id) + count(distinct file1.id) as Files
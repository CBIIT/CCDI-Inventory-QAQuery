MATCH (st:study)<-[:of_participant]-(p:participant)
WHERE st.phs_accession IN ['phs003111'] 
optional MATCH (st)<--(file1:clinical_measure_file)
optional Match (p)<--(file2)
where (file2: clinical_measure_file OR file2:radiology_file)
        MATCH (p)<-[:of_diagnosis]-(dg:diagnosis)
        where  dg.diagnosis_basis = 'Not Reported'
        MATCH (p)<-[*..3]-(sm:sample)
        where sm.sample_tumor_status ='Tumor'
        OPTIONAL MATCH (sm)<--(file)
        WHERE (file: sequencing_file OR file:pathology_file OR file:methylation_array_file OR file:single_cell_sequencing_file OR file:cytogenomic_file)
        WITH file,file1,file2, p, st, sm, dg
        return    count(distinct st.id) as Studies,
        count(distinct p.id)as Participants,
          count(distinct sm.id) as Samples,
          count(distinct dg.id) as Diagnosis,
        count(distinct file.id)+count(distinct file1.id) + count(distinct file2.id) as Files
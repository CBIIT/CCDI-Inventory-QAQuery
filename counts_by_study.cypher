        MATCH (st:study)<-[:of_participant]-(p:participant)
        where st.phs_accession='phs002517' 
        with st, count(p) as num_p
        MATCH (st)<-[:of_participant]-(p)<-[:of_diagnosis]-(dg:diagnosis)

        with st, num_p, dg.diagnosis_classification as dg_cancers, count(dg.diagnosis_classification) as num_cancers, count(dg.id) as count_of_diag
        ORDER BY num_cancers desc
        with st, num_p, collect(dg_cancers + ' (' + toString(num_cancers) + ')') as cancers, sum(count_of_diag) as num_of_diag
        MATCH (st)<-[*..5]-(fl)
        WHERE (fl:clinical_measure_file OR fl: sequencing_file OR fl:pathology_file OR fl:radiology_file OR fl:methylation_array_file OR fl:single_cell_sequencing_file OR fl:cytogenomic_file)
        with st, num_p, cancers, fl.file_type as ft, count(fl.file_type) as num_ft, num_of_diag
        ORDER BY num_ft desc
        with st, num_p, cancers,  collect(ft + ' (' + toString(num_ft) + ')') as file_types, sum(num_ft) as num_files, num_of_diag
        OPTIONAL MATCH (st)<-[:of_participant|of_cell_line|of_pdx]-(pcp)<-[:of_sample]-(sm1:sample)
        WHERE (pcp:participant or pcp:cell_line or pcp:pdx)
        WITH st, num_p, cancers,  file_types, num_files, count(distinct sm1.sample_id) as num_samples_1, num_of_diag
        OPTIONAL MATCH (st)<-[:of_participant]-(participant)<-[:of_sample]-(sm1:sample)<--(cp)<--(sm2:sample)
        WHERE (cp:cell_line or cp:pdx)
        WITH st, num_p, cancers,  file_types, num_files, num_samples_1, count(distinct sm2.sample_id) as num_samples_2, num_of_diag
        WITH st, num_p, cancers,  file_types, num_files, num_samples_1 + num_samples_2 as num_samples, num_of_diag
        MATCH (st)<-[*..5]-(file)
        WHERE (file:clinical_measure_file OR file: sequencing_file OR file:pathology_file OR file:radiology_file OR file:methylation_array_file OR file:single_cell_sequencing_file OR file:cytogenomic_file)
        OPTIONAL MATCH (st)<-[:of_publication]-(pub:publication)
        OPTIONAL MATCH (st)<-[:of_study_personnel]-(stp:study_personnel)
        WHERE stp.personnel_type = 'PI'
        OPTIONAL MATCH (st)<-[:of_study_funding]-(stf:study_funding)
                
        WITH st, num_p, cancers,  file_types, num_files, num_samples, file.id as file_id, stf, stp, pub, num_of_diag
        RETURN DISTINCT
          st.id as id,
          st.study_id as study_id,
          st.phs_accession as phs_accession,
          num_p as num_of_participants,   
          num_samples as num_of_samples,
          num_files as num_of_files,
           num_of_diag as  num_of_diag
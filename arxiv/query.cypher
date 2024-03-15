        MATCH (st:study)<-[:of_participant]-(p:participant)
        with st, count(p) as num_p
        MATCH (st:study)<-[:of_participant]-(participant)<-[:of_diagnosis]-(dg:diagnosis)
        with st, num_p, dg.diagnosis_classification as dg_cancers, count(dg.diagnosis_classification) as num_cancers
        ORDER BY num_cancers desc
        with st, num_p, collect(dg_cancers + ' (' + toString(num_cancers) + ')') as cancers
        MATCH (st)<-[:of_participant]-(pa:participant)<-[:of_diagnosis]-(diag:diagnosis)
        with st, num_p, cancers, diag.anatomic_site as dg_sites, count(diag.anatomic_site) as num_sites
        ORDER BY num_sites desc
        with st, num_p, cancers, collect(dg_sites + ' (' + toString(num_sites) + ')') as sites
        MATCH (st)<-[*..5]-(fl)
        WHERE (fl:clinical_measure_file OR fl: sequencing_file OR fl:pathology_file OR fl:radiology_file OR fl:methylation_array_file OR fl:single_cell_sequencing_file OR fl:cytogenomic_file)
        with st, num_p, cancers, sites, fl.file_type as ft, count(fl.file_type) as num_ft
        ORDER BY num_ft desc
        with st, num_p, cancers, sites, collect(ft + ' (' + toString(num_ft) + ')') as file_types, sum(num_ft) as num_files
        OPTIONAL MATCH (st)<-[:of_participant|of_cell_line|of_pdx]-(pcp)<-[:of_sample]-(sm1:sample)
        WHERE (pcp:participant or pcp:cell_line or pcp:pdx)
        WITH st, num_p, cancers, sites, file_types, num_files, count(distinct sm1.sample_id) as num_samples_1
        OPTIONAL MATCH (st)<-[:of_participant]-(participant)<-[:of_sample]-(sm1:sample)<--(cp)<--(sm2:sample)
        WHERE (cp:cell_line or cp:pdx)
        WITH st, num_p, cancers, sites, file_types, num_files, num_samples_1, count(distinct sm2.sample_id) as num_samples_2
        WITH st, num_p, cancers, sites, file_types, num_files, num_samples_1 + num_samples_2 as num_samples
        MATCH (st)<-[*..5]-(file)
        WHERE (file:clinical_measure_file OR file: sequencing_file OR file:pathology_file OR file:radiology_file OR file:methylation_array_file OR file:single_cell_sequencing_file OR file:cytogenomic_file)
        OPTIONAL MATCH (st)<-[:of_publication]-(pub:publication)
        OPTIONAL MATCH (st)<-[:of_study_personnel]-(stp:study_personnel)
        WHERE stp.personnel_type = 'PI'
        OPTIONAL MATCH (st)<-[:of_study_funding]-(stf:study_funding)
        WITH st, num_p, cancers, sites, file_types, num_files, num_samples, file.id as file_id, stf, stp, pub
        RETURN DISTINCT
          st.id as id,
          st.study_id as study_id,
          st.phs_accession as phs_accession,
          num_p as num_of_participants,   
          file_types as file_types,
          num_samples as num_of_samples,
          num_files as num_of_files
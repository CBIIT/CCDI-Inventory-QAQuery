MATCH (st:study)<-[:of_participant]-(p:participant)
WHERE apoc.coll.contains(apoc.text.split(p.ethnicity,";"), 'Hispanic or Latino') and st.phs_accession IN ['phs003111']
        MATCH (p)<-[:of_diagnosis]-(dg:diagnosis)
        where dg.anatomic_site='C74.9 : Adrenal gland, NOS' and dg.diagnosis_classification='9500/3 : Neuroblastoma, NOS'
        optional MATCH (p)<-[:of_sample]-(sm1:sample)<-[*0..2]-(sm2:sample)<--(file3)
        WHERE (file3: sequencing_file OR file3:pathology_file OR file3:methylation_array_file OR file3:single_cell_sequencing_file OR file3:cytogenomic_file)
        WITH  st, p, collect(distinct {participant_id: p.participant_id, sample_id: CASE sm1.sample_id WHEN sm2.sample_id THEN sm2.sample_id
        ELSE sm1.sample_id + ',' + sm2.sample_id END, file: file3}) as file_set_3
        optional MATCH (p)<--(file2)
        where (file2:radiology_file or file2: clinical_measure_file)
        with st, p, file_set_3, collect({participant_id: p.participant_id, sample_id: null, file: file2}) as file_set_2
        with st, apoc.coll.union(file_set_3, file_set_2) as file_set
        optional MATCH (st)<--(file1:clinical_measure_file)
        with st, file_set, collect({participant_id: null, sample_id: null, file: file1}) as file_set_1
        with st, file_set + file_set_1 as file_set
        UNWIND  file_set AS file_entry
        WITH file_entry.file as fileRow, st.study_id as study_id, file_entry.participant_id as participant_id, file_entry.sample_id as sample_id
RETURN
  fileRow.file_name AS `File Name`,
  CASE
    WHEN 'sequencing_file' IN LABELS(fileRow) THEN 'Sequencing'
    WHEN 'single_cell_sequencing_file' IN LABELS(fileRow) THEN 'Single Cell Sequencing'
    WHEN 'cytogenomic_file' IN LABELS(fileRow) THEN 'Cytogenomic'
    WHEN 'pathology_file' IN LABELS(fileRow) THEN 'Pathology imaging'
    WHEN 'methylation_array_file' IN LABELS(fileRow) THEN 'Methylation array'
    WHEN 'clinical_measure_file' IN LABELS(fileRow) THEN 'Clinical data'
    WHEN 'radiology_file' IN LABELS(fileRow) THEN 'Radiology imaging'
    ELSE ''
  END AS `File Category`,
  fileRow.file_description AS `File Description`,
  fileRow.file_type AS `File Type`,
  fileRow.file_size AS `File Size`,
  study_id AS `Study ID`,
  participant_id AS `Participant ID`,
  sample_id AS `Sample ID`,
  fileRow.dcf_indexd_guid AS `GUID`,
  fileRow.md5sum AS `MD5Sum`
ORDER BY `File Name`
LIMIT 100
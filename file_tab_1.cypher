MATCH (st:study)<-[:of_participant]-(p:participant)
WHERE p.ethnicity IN ['Hispanic or Latino']  and st.phs_accession IN ['phs003111']
        OPTIONAL MATCH (p)<-[*..4]-(file)
        WHERE (file:clinical_measure_file OR file: sequencing_file OR file:pathology_file OR file:radiology_file OR file:methylation_array_file OR file:single_cell_sequencing_file OR file:cytogenomic_file)
        optional MATCH (st)<--(file1:clinical_measure_file)
        MATCH (p)<-[:of_diagnosis]-(dg:diagnosis)
        where dg.anatomic_site='C74.9 : Adrenal gland, NOS' and dg.diagnosis_classification='9500/3 : Neuroblastoma, NOS'
        OPTIONAL MATCH (p)<-[*..3]-(sm:sample)
        WITH  p, st, sm, dg,  collect(distinct file) + collect(distinct file1) as cf   
            UNWIND  cf AS fileRow1
        WITH DISTINCT fileRow1 as fileRow, st, p, sm, dg
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
  st.study_id AS `Study ID`,
  p.participant_id AS `Participant ID`,
  sm.sample_id AS `Sample ID`,
  fileRow.dcf_indexd_guid AS `GUID`,
  fileRow.md5sum AS `MD5Sum`
ORDER BY `File Name`
LIMIT 100
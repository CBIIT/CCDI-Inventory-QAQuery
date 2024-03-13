MATCH (st:study)<-[:of_participant]-(p:participant)
where st.phs_accession in ["phs002430"]
with st, count(p) as num_p
MATCH (st)<-[:of_participant]-(participant)<-[:of_diagnosis]-(dg:diagnosis)
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
order by st.study_id
RETURN DISTINCT
  st.study_short_title as `Study Short Title`,
st.study_id as `Study ID`,
  CASE WHEN size(cancers) > 5 THEN apoc.text.join(apoc.coll.remove(cancers, 5, 10000), "\n") + "\nRead More"  else apoc.text.join(cancers, "\n") END as `Diagnosis (Top 5)`,
  CASE WHEN size(sites) > 5 THEN apoc.text.join(apoc.coll.remove(sites, 5, 10000), "\n") + "\nRead More"  else apoc.text.join(sites, "\n") END as `Diagnosis Anatomic Site (Top 5)`,
  num_p as `Number of Participants`,
  num_samples as `Number of Samples`,
  num_files as `Number of Files`,
  CASE WHEN size(file_types) > 5 THEN apoc.text.join(apoc.coll.remove(file_types, 5, 10000), "\n") + "\nRead More"  else apoc.text.join(file_types, "\n") END as `File Type (Top 5)`,
  apoc.text.join(COLLECT(DISTINCT pub.pubmed_id), ';') as `PubMed ID`,
  apoc.text.join(COLLECT(DISTINCT stp.personnel_name), ';') as `Principal Investigator(s)`,
  apoc.text.join(COLLECT(DISTINCT stf.grant_id), ';') as `Grant ID`
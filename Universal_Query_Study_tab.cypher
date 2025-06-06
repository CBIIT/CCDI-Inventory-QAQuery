Match (st:study)
where st.dbgap_accession in ['']
optional match (st)<-[*..6]-(file)
where (file:clinical_measure_file or file:generic_file or file:radiology_file or file:sequencing_file or file:pathology_file or file:methylation_array_file or file:cytogenomic_file)
with st, file,
        apoc.text.split(file.data_category, ';') as file_c,
        file.file_type as file_t,
        file.file_mapping_level as file_ml,
        CASE LABELS(file)[0]
                        WHEN 'sequencing_file' THEN file.library_selection
                        ELSE null END as library_s,
        CASE LABELS(file)[0]
                        WHEN 'sequencing_file' THEN file.library_source
                        ELSE null END as library_source_mat,
        CASE LABELS(file)[0]
                        WHEN 'sequencing_file' THEN file.library_source
                        ELSE null END as library_source_mol,
        CASE LABELS(file)[0]
                        WHEN 'sequencing_file' THEN file.library_strategy
                        ELSE null END as library_str
with st, apoc.coll.flatten(collect(distinct file_c)) as data_category, collect(distinct file_t) as file_type, collect(distinct file_ml) as file_mapping_level, collect(distinct library_s) as library_selection, collect(distinct library_source_mat) as library_source_material, collect(distinct library_source_mol) as library_source_molecule, collect(distinct library_str) as library_strategy
where st.study_acronym in [''] and st.study_name in [''] and st.study_status in ['']
        and ANY(element IN [''] WHERE element IN data_category) and ANY(element IN [''] WHERE element IN file_type) and ANY(element IN [''] WHERE element IN file_mapping_level)
        and ANY(element IN [''] WHERE element IN library_selection) and ANY(element IN [''] WHERE element IN library_source_material) and ANY(element IN [''] WHERE element IN library_source_molecule) and ANY(element IN [''] WHERE element IN library_strategy)
with distinct st
optional match (st)<--(p:participant)
with st, p, apoc.text.split(p.race, ',') as race
where p.participant_id in [''] and p.sex_at_birth in [''] and ANY(element IN [''] WHERE element IN race)
with distinct st
optional match (st)<-[*..6]-(dg:diagnosis)
with st, dg, apoc.text.split(dg.anatomic_site, ';') as diagnosis_anatomic_site
where dg.age_at_diagnosis >= [''] and dg.age_at_diagnosis <= [''] and dg.diagnosis in [''] and ANY(element IN [''] WHERE element IN diagnosis_anatomic_site) and dg.diagnosis_classification_system in [''] and dg.diagnosis_basis in [''] and dg.disease_phase in ['']
with distinct st
optional match (st)<-[*..5]-(sm:sample)
with st, sm, apoc.text.split(sm.anatomic_site, ';') as sample_anatomic_site
where sm.participant_age_at_collection >= [''] and sm.participant_age_at_collection <= [''] and ANY(element IN [''] WHERE element IN sample_anatomic_site) and sm.sample_tumor_status in [''] and sm.tumor_classification in [''] 
with distinct st
optional match (st)<--(p:participant)<--(su:survival)
where su.last_known_survival_status in [''] and su.event_free_survival_status in ['']
        and su.first_event in [''] and su.age_at_last_known_survival_status >= [''] and su.age_at_last_known_survival_status <= ['']
with distinct st
optional match (st)<--(p:participant)<--(tm:treatment)
with st, p, tm.age_at_treatment_start as age_at_treatment_start, apoc.text.split(tm.treatment_type, ';') as treatment_type, apoc.text.split(tm.treatment_agent, ';') as treatment_agent
where ANY(element IN [''] WHERE element IN treatment_type) and ANY(element IN [''] WHERE element IN treatment_agent) and age_at_treatment_start >= [''] and age_at_treatment_start <= ['']
with distinct st
optional match (st)<--(p:participant)<--(tr:treatment_response)
where tr.response_category in [''] and tr.age_at_response >= [''] and tr.age_at_response <= ['']
with distinct st
MATCH (st)<-[:of_participant]-(p:participant)
with st, count(p) as num_p
MATCH (st:study)<-[*..5]-(dg:diagnosis)
with st, num_p, dg.diagnosis as dg_cancers, count(dg.diagnosis) as num_cancers
ORDER BY num_cancers desc
with st, num_p, collect(dg_cancers + ' (' + toString(num_cancers) + ')') as cancers
MATCH (st)<-[*..5]-(diag:diagnosis)
with st, num_p, cancers, apoc.text.split(diag.anatomic_site, ';') as dg_sites
unwind dg_sites as dg_site
with st, num_p, cancers, dg_site
with st, num_p, cancers, dg_site, count(dg_site) as num_sites
ORDER BY num_sites desc
with st, num_p, cancers, collect(dg_site + ' (' + toString(num_sites) + ')') as sites
MATCH (st)<-[*..5]-(fl)
WHERE (fl:clinical_measure_file or fl:generic_file OR fl: sequencing_file OR fl:pathology_file OR fl:radiology_file OR fl:methylation_array_file OR fl:cytogenomic_file)
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
WHERE (file:clinical_measure_file or file:generic_file OR file: sequencing_file OR file:pathology_file OR file:radiology_file OR file:methylation_array_file OR file:cytogenomic_file)
OPTIONAL MATCH (st)<-[:of_publication]-(pub:publication)
OPTIONAL MATCH (st)<-[:of_study_personnel]-(stp:study_personnel)
WHERE stp.personnel_type = 'PI'
OPTIONAL MATCH (st)<-[:of_study_funding]-(stf:study_funding)
WITH st, num_p, cancers, sites, file_types, num_files, num_samples, file.id as file_id, stf, stp, pub
with DISTINCT
st.id as id,
st.study_id as study_id,
st.study_status as study_status,
apoc.text.join(COLLECT(DISTINCT stf.grant_id), ';') as grant_id,
apoc.text.join(COLLECT(DISTINCT pub.pubmed_id), ';') as pubmed_ids,
st.dbgap_accession as dbgap_accession,
st.study_name as study_name,
st.study_status as study_status,
st.study_acronym as study_acronym,
apoc.text.join(COLLECT(DISTINCT stp.personnel_name), ';') as PIs,
num_p as num_of_participants,
cancers as diagnosis_cancer,
sites as diagnosis_anatomic_site,
file_types as file_types,
num_samples as num_of_samples,
num_files as num_of_files
RETURN DISTINCT
study_name as `Study Name`,
study_id as `Study ID`,
study_status as `Study Status`,
CASE WHEN size(diagnosis_cancer) > 5 THEN apoc.text.join(apoc.coll.remove(diagnosis_cancer, 5, 10000), '\n') + '\nRead More'  else apoc.text.join(diagnosis_cancer, '\n') END as `Diagnosis (Top 5)`,
CASE WHEN size(diagnosis_anatomic_site) > 5 THEN apoc.text.join(apoc.coll.remove(diagnosis_anatomic_site, 5, 10000), '\n') + '\nRead More'  else apoc.text.join(diagnosis_anatomic_site, '\n') END as `Diagnosis Anatomic Site (Top 5)`,
num_of_participants as `Number of Participants`,
num_of_samples as `Number of Samples`,
num_of_files as `Number of Files`,
CASE WHEN size(file_types) > 5 THEN apoc.text.join(apoc.coll.remove(file_types, 5, 10000), '\n') + '\nRead More'  else apoc.text.join(file_types, '\n') END as `File Type (Top 5)`,
pubmed_ids as `PubMed ID`,
PIs as `Principal Investigator(s)`,
grant_id as `Grant ID`
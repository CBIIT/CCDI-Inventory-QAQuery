/* Diagnosis Section */

// diagnosis facet
match (dg:diagnosis)
return distinct dg.diagnosis as diagnosis
order by diagnosis

// diagnosis anatomic site facet
match (dg:diagnosis)
with distinct dg.anatomic_site as diagnosis_anatomic_site_str
with apoc.text.split(diagnosis_anatomic_site_str, ';') as arr
unwind arr as anatomic_site
return distinct anatomic_site as diagnosis_anatomic_site
order by diagnosis_anatomic_site

// diagnosis classification system facet
match (dg:diagnosis)
return distinct dg.diagnosis_classification_system as diagnosis_classification_system
order by diagnosis_classification_system

// diagnosis basis facet
match (dg:diagnosis)
return distinct dg.diagnosis_basis as diagnosis_basis
order by diagnosis_basis

// disease phase facet
match (dg:diagnosis)
return distinct dg.disease_phase as disease_phase
order by disease_phase


/* Demographics Section */

// sex facet
match (p:participant)
return distinct p.sex_at_birth as sex_at_birth
order by sex_at_birth

// race facet
match (p:participant)
with distinct p.race as race_str
with apoc.text.split(race_str, ';') as arr
unwind arr as race
return distinct race as race
order by race


/* Treatment Section */

// treatment agent facet
match (t:treatment)
with distinct t.treatment_agent as treatment_agent_str
where trim(treatment_agent_str) <> ''
with apoc.text.split(treatment_agent_str, ';') as arr
unwind arr as treatment_agent
return distinct treatment_agent as treatment_agent
order by treatment_agent

// treatment type facet
match (t:treatment)
with distinct t.treatment_type as treatment_type_str
where trim(treatment_type_str) <> ''
with apoc.text.split(treatment_type_str, ';') as arr
unwind arr as treatment_type
return distinct treatment_type as treatment_type
order by treatment_type


/* Treatment Response Section */

// response category facet
match (tr:treatment_response)
return distinct tr.response_category as response_category
order by response_category


/* Survival Section */

// first event facet
match (sur:survival)
where trim(sur.first_event) <> ''
return distinct sur.first_event as first_event
order by first_event

// last known survival status facet
match (sur:survival)
return distinct sur.last_known_survival_status as last_known_survival_status
order by last_known_survival_status


/* Samples Section */

// sample anatomic site facet
match (sam:sample)
with distinct sam.anatomic_site as anatomic_site_str
where trim(anatomic_site_str) <> ''
with apoc.text.split(anatomic_site_str, ';') as arr
unwind arr as anatomic_site
return distinct anatomic_site as sample_anatomic_site
order by sample_anatomic_site

// sample tumor status facet
match (sam:sample)
return distinct sam.sample_tumor_status as sample_tumor_status
order by sample_tumor_status

//tumor classification facet
match (sam:sample)
return distinct sam.tumor_classification as tumor_classification
order by tumor_classification


/* Data Category Section */

// data category facet
match (file)
where (file:clinical_measure_file OR file: sequencing_file OR file:pathology_file OR file:radiology_file OR file:methylation_array_file OR file:cytogenomic_file OR file:generic_file)
with distinct file.data_category as data_category_str
where trim(data_category_str) <> ''
with apoc.text.split(data_category_str, ';') as arr
unwind arr as data_category
return distinct data_category as data_category
order by toLower(data_category)

// file type facet
match (file)
where (file:clinical_measure_file OR file: sequencing_file OR file:pathology_file OR file:radiology_file OR file:methylation_array_file OR file:cytogenomic_file OR file:generic_file)
return distinct file.file_type as file_type
order by file_type

// file mapping level facet
match (file)
where (file:clinical_measure_file OR file: sequencing_file OR file:pathology_file OR file:radiology_file OR file:methylation_array_file OR file:cytogenomic_file OR file:generic_file)
return distinct file.file_mapping_level as file_mapping_level
order by file_mapping_level


/* Study Section */

// dbgap accession facet
match (st:study)
return distinct st.dbgap_accession as dbgap_accession
order by dbgap_accession

// study name facet
match (st:study)
return distinct st.study_name as study_name
order by study_name

// study status facet
match (st:study)
return distinct st.study_status as study_status
order by study_status


/* Sequencing Library Section */

// library selection facet
match (file)
where (file:clinical_measure_file OR file: sequencing_file OR file:pathology_file OR file:radiology_file OR file:methylation_array_file OR file:cytogenomic_file OR file:generic_file)
with distinct file.library_selection as library_selection
where library_selection is not null
return library_selection
order by tolower(library_selection)

// library source material facet
match (file)
where (file:clinical_measure_file OR file: sequencing_file OR file:pathology_file OR file:radiology_file OR file:methylation_array_file OR file:cytogenomic_file OR file:generic_file)
with distinct file.library_source_material as library_source_material
where library_source_material is not null
return library_source_material
order by tolower(library_source_material)

// library strategy facet
match (file)
where (file:clinical_measure_file OR file: sequencing_file OR file:pathology_file OR file:radiology_file OR file:methylation_array_file OR file:cytogenomic_file OR file:generic_file)
with distinct file.library_strategy as library_strategy
where library_strategy is not null
return library_strategy
order by tolower(library_strategy)

// library source molecule facet
match (file)
where (file:clinical_measure_file OR file: sequencing_file OR file:pathology_file OR file:radiology_file OR file:methylation_array_file OR file:cytogenomic_file OR file:generic_file)
with distinct file.library_source_molecule as library_source_molecule
where library_source_molecule is not null
return library_source_molecule
order by tolower(library_source_molecule)

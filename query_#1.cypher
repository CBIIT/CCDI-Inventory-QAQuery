MATCH (s:study)<--(p:participant)<--(dia: diagnosis),--(sm)<-(file:)
option match (s)<--[*..1]<-(sm:smaple)
optional match (s)<--[]
WHERE sm.smpel_anomic... s.phs_accession IN ['phs003111'] and p.ethnicity IN ['Hispanic or Latino'] and dia.anatomic_site in ['C74.9 : Adrenal gland, NOS'] and dia.diagnosis_classification in ['9500/3 : Neuroblastoma, NOS']
WITH DISTINCT p, s
RETURN
coalesce(p.participant_id, '') AS `Participant ID`,
coalesce(s.phs_accession, '') AS `Study ID`,
coalesce(p.sex_at_birth, '') AS `Sex` ,
coalesce(p.race, '') AS `Race`,
coalesce(p.ethnicity, '') AS `Ethnicity` ,
coalesce(p.alternate_participant_id, '') AS `Alternate ID`
Order by p.participant_id Limit 100

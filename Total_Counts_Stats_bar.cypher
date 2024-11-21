MATCH (m:study)
WITH COUNT(m) AS study_count
MATCH (p:participant)
WITH study_count, COUNT(p) AS participant_count
MATCH (s:sample)
WITH study_count, participant_count, COUNT(s) AS sample_count
MATCH (n)
WHERE ANY(label IN labels(n) WHERE toLower(label) CONTAINS "file" and (not (toLower(label) CONTAINS "generic_file")) )
RETURN study_count, participant_count, sample_count, COUNT(n) AS file_count;

MATCH (st:study)
WHERE st.dbgap_accession = "phs001437"
 
OPTIONAL MATCH (st)<--(cl)<--(sm:sample)<--(dg1:diagnosis)
WITH st, COLLECT(DISTINCT dg1.diagnosis_id) AS dgs1
 
OPTIONAL MATCH (st)<--(p:participant)<--(dg2:diagnosis)
WITH st, dgs1 + COLLECT(DISTINCT dg2.diagnosis_id) AS dgs2
 
OPTIONAL MATCH (st)<--(p2:participant)<-[:of_sample]-(sm1:sample)<--(cl2)<--(sm2:sample)<--(dg3:diagnosis)
WITH st, dgs2 + COLLECT(DISTINCT dg3.diagnosis_id) AS dgs3
 
OPTIONAL MATCH (st)<--(p3:participant)<--(sm3:sample)<--(dg4:diagnosis)
WITH dgs3 + COLLECT(DISTINCT dg4.diagnosis_id) AS all_diagnosis_ids
 
UNWIND all_diagnosis_ids AS diagnosis_id
WITH DISTINCT diagnosis_id
RETURN count(diagnosis_id) AS merged_diagnosis_count
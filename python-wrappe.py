import os
import re
from neo4j import GraphDatabase

class Neo4jQueryExecutor:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
    
    def close(self):
        self.driver.close()
    
    def execute_query_from_file(self, query_file, **filters):
        """
        Execute a query from a file with specified filters.
        :param query_file: Path to the file containing the Cypher query.
        :param filters: The filter conditions as keyword arguments.
        :return: The result of the query.
        """
        def _build_query(query_template, filters):
            """
            Dynamically replaces filter placeholders and removes unused conditions.
            """
            # Regex to find WHERE/AND conditions with placeholders
            condition_pattern = re.compile(r"(WHERE|AND)\s+([a-zA-Z0-9_.]+)\s+(IN|=|>=|<=)\s+\[(.*?)\]")
            matches = condition_pattern.findall(query_template)
            
            for full_match in matches:
                clause_type, field, operator, placeholder = full_match
                field_key = field.strip()
                
                if field_key in filters and filters[field_key] is not None:
                    # Replace the placeholder with actual filter values
                    value = filters[field_key]
                    if isinstance(value, list):
                        value_str = ", ".join([f"'{v}'" for v in value])
                        replacement = f"{clause_type} {field} {operator} [{value_str}]"
                    else:
                        replacement = f"{clause_type} {field} {operator} '{value}'"
                    query_template = query_template.replace(f"{clause_type} {field} {operator} [{placeholder}]", replacement)
                else:
                    # Remove the condition
                    query_template = query_template.replace(f"{clause_type} {field} {operator} [{placeholder}]", "")
            
            # Remove orphaned WHERE/AND
            query_template = re.sub(r"\bWHERE\s*$", "", query_template, flags=re.MULTILINE)
            query_template = re.sub(r"\bAND\s*$", "", query_template, flags=re.MULTILINE)
            query_template = re.sub(r"\bWHERE\s+AND\b", "WHERE", query_template, flags=re.MULTILINE)

            return query_template.strip()

        # Read the Cypher query from the file
        with open(query_file, 'r') as file:
            query_template = file.read()
        
        query = _build_query(query_template, filters)
        print("Executing Query:\n", query)  # Debug output for verification
        with self.driver.session() as session:
            result = session.run(query)
            return [record.data() for record in result]

# Usage example:

# Replace these with your Neo4j connection details
uri = "bolt://10.208.9.97:7687"
user = "neo4j"
password = "neo4j1"

query_executor = Neo4jQueryExecutor(uri, user, password)

# Path to the Cypher query file
query_file = os.path.join(os.getcwd(), "Universal_Query_Diagnosis_tab.cypher")

# Specify filter conditions (leave unspecified filters as None or empty lists)
filters = {
    "st.dbgap_accession": ["phs000470", "phs000468"],
    "age_at_diagnosis": [0, 14570],  # Example range filter
    "participant_id": None  # Example of a bypass filter
}

results = query_executor.execute_query_from_file(query_file, **filters)

# Print results
for result in results:
    print(result)

query_executor.close()

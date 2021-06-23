# Registry Concept Script OMOPv6.0
The Registry Concept script will process an excel file and either update or insert a new concept id base on a criteria setup by the config file. 

## Set up config.ini
**[excel]**
- name=input.xlsx
- sheet=Sheet1

**[postgresql]**
- host=127.0.0.1
- dbname=postgres
- port=5432
- user=postgres
- password=password

**[database]**
- start_index=2100000000

**[default]**
- concept_id=0
- concept_name='Observation'
- domain_id='Observation'
- vocabulary_id='Vocabulary'
- concept_class_id='Observation'
- standard_concept='S'
- concept_code='A0030'
- valid_start_date='2021-01-01'
- valid_end_date='2099-12-31'
- invalid_reason='N'


**[mapping]**
- concept_id=Element OMOP Concept ID
- concept_name=Element OMOP Concept Name
- vocabulary=Vocabulary
- concept_class_id=Concept Class
- concept_code=Element OMOP Concept Code

import pandas as pd
from neo4j import GraphDatabase

# Neo4j 연결 설정
driver = GraphDatabase.driver("bolt://localhost:7688", auth=None)

# CSV 파일 읽기
csv_file_path = "/home/a202121010/data/law/output.csv"
df = pd.read_csv(csv_file_path, nrows=50)
df = df.fillna('')

# 통합된 노드 생성 함수들
def create_case_info(tx, row):
    tx.run("""
        MERGE (c:CaseInfo {
            caseNo: $case_no,
            caseName: $case_name,
            caseField: $case_field,
            detailField: $detail_field,
            trialField: $trial_field,
            courtName: $court_name,
            judgmentDate: $judgment_date
        })
    """, case_no=row['info.caseNo'],
          case_name=row['info.caseNm'],
          case_field=row['info.caseField'],
          detail_field=row['info.detailField'],
          trial_field=row['info.trailField'],
          court_name=row['info.courtNm'],
          judgment_date=row['info.judmnAdjuDe'])

def create_concerned_parties(tx, row, case_no):
    tx.run("""
        MATCH (c:CaseInfo {caseNo: $case_no})
        MERGE (p:ConcernedParties {
            plaintiff: $plaintiff,
            defendant: $defendant
        })
        MERGE (c)-[:HAS_PARTIES]->(p)
    """, case_no=case_no,
          plaintiff=row['concerned.acusr'],
          defendant=row['concerned.dedat'])

def create_disposal_info(tx, row, case_no):
    tx.run("""
        MATCH (c:CaseInfo {caseNo: $case_no})
        MERGE (d:DisposalInfo {
            form: $form,
            content: $content
        })
        MERGE (c)-[:HAS_DISPOSAL]->(d)
    """, case_no=case_no,
          form=row['disposal.disposalform'],
          content=row['disposal.disposalcontent'])

def create_facts_decision(tx, row, case_no):
    tx.run("""
        MATCH (c:CaseInfo {caseNo: $case_no})
        MERGE (f:FactsAndDecision {
            facts: $facts,
            decision: $decision
        })
        MERGE (c)-[:HAS_FACTS_DECISION]->(f)
    """, case_no=case_no,
          facts=row['facts.bsisFacts'],
          decision=row['dcss.courtDcss'])

def create_conclusion(tx, row, case_no):
    tx.run("""
        MATCH (c:CaseInfo {caseNo: $case_no})
        MERGE (cl:Conclusion {
            request: $request,
            conclusion: $conclusion
        })
        MERGE (c)-[:HAS_CONCLUSION]->(cl)
    """, case_no=case_no,
          request=row['mentionedItems.rqestObjet'],
          conclusion=row['close.cnclsns'])

# 단독 노드 생성 함수들
def create_related_law(tx, law_name, case_no):
    tx.run("""
        MATCH (c:CaseInfo {caseNo: $case_no})
        MERGE (l:RelatedLaw {name: $law_name})
        MERGE (c)-[:REFERENCES_LAW]->(l)
    """, law_name=law_name, case_no=case_no)

def create_precedent(tx, precedent, case_no):
    tx.run("""
        MATCH (c:CaseInfo {caseNo: $case_no})
        MERGE (p:Precedent {name: $precedent})
        MERGE (c)-[:CITES_PRECEDENT]->(p)
    """, precedent=precedent, case_no=case_no)

# Neo4j에 데이터 적재
with driver.session() as session:
    for idx, row in df.iterrows():
        print(f"{idx + 1}번째 데이터 적재 중...")
        with session.begin_transaction() as tx:
            try:
                case_no = row['info.caseNo']
                
                # 통합 노드들 생성
                create_case_info(tx, row)
                create_concerned_parties(tx, row, case_no)
                create_disposal_info(tx, row, case_no)
                create_facts_decision(tx, row, case_no)
                create_conclusion(tx, row, case_no)
                
                # 단독 노드들 생성
                if row['info.relateLaword']:
                    for law in str(row['info.relateLaword']).split(','):
                        create_related_law(tx, law.strip(), case_no)
                
                if row['info.qotatPrcdnt']:
                    for precedent in str(row['info.qotatPrcdnt']).split(','):
                        create_precedent(tx, precedent.strip(), case_no)
                
                tx.commit()
            except Exception as e:
                tx.rollback()
                print(f"Error occurred: {e}")

driver.close()
print("데이터가 Neo4j에 성공적으로 적재되었습니다.")
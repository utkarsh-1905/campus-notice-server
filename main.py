from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy import create_engine, Column, Integer, Float, String, ARRAY, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from pydantic import BaseModel
from typing import List, Dict, Any
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

def create_database_url(username, password, host, database_name) -> str:
    db_url = f"postgresql://{username}:{password}@{host}:{5432}/{database_name}"
    print(f"Database URL: {db_url}")
    return db_url

db = create_database_url(os.getenv("DB_USERNAME"), os.getenv("DB_PASSWORD"), os.getenv("DB_HOST"), "CampusNotice")

engine = create_engine(db)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# SQLAlchemy Model
class Company(Base):
    __tablename__ = "companies"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    cgpa = Column(Float, nullable=False)
    deadline = Column(DateTime, nullable=False)
    form_link = Column(String, nullable=False)
    profiles = Column(String, nullable=False)
    branches = Column(ARRAY(String), nullable=False)

# Create the tables in the database
Base.metadata.create_all(bind=engine)

# Pydantic Model for incoming data
class ParsePayload(BaseModel):
    candidates: List[Dict[str, Dict[str, List[Dict[str, str]]]]]

class CompanyResponse(BaseModel):
    id: int
    name: str
    cgpa: float
    deadline: datetime
    form_link: str
    profiles: str
    branches: List[str]

    class Config:
        orm_mode = True

# FastAPI app initialization
app = FastAPI()

# Dependency to get DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Parse Function
def parse(d: Any) -> Any:
    content = d.get("candidates")[0].get("content").get("parts")[0].get("text")
    # content = d["candidates"][0]["content"]["parts"][0]["text"]
    content = content.replace("```json\n", "").replace("\n```", "")
    content = content.replace(" ===", "===")
    content = content.replace("=== ", "===")
    content = content.replace("\n", "")
    content = content.replace("[", "")
    content = content.replace("]", "")
    content = content.replace("\"", "")
    content = content.split(",")

    companies = []
    for line in content:
        parts = line.split("===")

        companies.append({
            "name": parts[0],
            "cgpa": parts[1],
            "deadline": parts[2],
            "form_link": parts[3],
            "branches": parts[4].split("/")
        })
    return companies

# POST route to parse and add companies to the database
@app.post("/")
async def create_companies_from_parsed(data, db: Session = Depends(get_db)):
    try:
        # Parse the incoming payload
        print(data.dict())
        companies = parse(data)
        # Add each company to the database
        for company in companies:
            db_company = Company(
                name=company["name"],
                cgpa=company["cgpa"],
                deadline=datetime.fromisoformat(company["deadline"].replace("Z", "+00:00")),
                form_link=company["form_link"],
                profiles="FTE Only",  # Set a default or add this information in the parsing
                branches=company["branches"]
            )
            db.add(db_company)
        db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    return {"message": "Companies parsed and added successfully"}

@app.get("/", response_model=List[CompanyResponse])
def get_closest_deadline_companies(db: Session = Depends(get_db)):
    try:
        companies = db.query(Company).order_by(Company.deadline).limit(1).all()
        if not companies:
            raise HTTPException(status_code=404, detail="No companies found")
        return companies
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# main.py
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from sqlalchemy import create_engine, Column, String, Float, Integer, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from pydantic import BaseModel
from typing import Optional, List
import pandas as pd
import os
from contextlib import contextmanager
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database configuration
DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Database Model
class PopulationData(Base):
    __tablename__ = "population_data"
    
    id = Column(Integer, primary_key=True, index=True)
    region_country_area = Column(String, index=True)
    population_density_surface_area = Column(Text, index=True)
    year = Column(String, index=True)
    series = Column(Text, index=True)
    value = Column(String)
    footnotes = Column(Text, nullable=True)
    source = Column(Text)

# Pydantic Models
class PopulationDataResponse(BaseModel):
    id: int
    region_country_area: str
    population_density_surface_area: str
    year: str
    series: str
    value: str
    footnotes: Optional[str]
    source: str
    
    class Config:
        from_attributes = True

class PopulationDataCreate(BaseModel):
    region_country_area: str
    population_density_surface_area: str
    year: str
    series: str
    value: str
    footnotes: Optional[str] = None
    source: str

class PopulationDataUpdate(BaseModel):
    region_country_area: Optional[str] = None
    population_density_surface_area: Optional[str] = None
    year: Optional[str] = None
    series: Optional[str] = None
    value: Optional[str] = None
    footnotes: Optional[str] = None
    source: Optional[str] = None

# Initialize FastAPI
app = FastAPI(title="Population Data API", version="1.0.0")

# Database session dependency
@contextmanager
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Create tables
@app.on_event("startup")
async def startup_event():
    Base.metadata.create_all(bind=engine)

# ============== FILE UPLOAD ENDPOINT (UNCHANGED) ==============
@app.post("/upload-csv/")
async def upload_csv(file_path: str):
    """
    Upload CSV file to PostgreSQL database
    """
    try:
        # Read CSV - ignore the index column if it exists
        df = pd.read_csv(file_path, index_col=0)
        
        # Reset index to avoid issues
        df = df.reset_index(drop=True)
        
        # Handle the specific column names from your CSV
        column_mapping = {
            'Region/Country/Area': 'region_country_area',
            'Population, density and surface area': 'population_density_surface_area',
            'Year': 'year',
            'Series': 'series',
            'Value': 'value',
            'Footnotes': 'footnotes',
            'Source': 'source'
        }
        
        df = df.rename(columns=column_mapping)
        
        # Select only the columns that exist in the database model
        valid_columns = ['region_country_area', 'population_density_surface_area', 
                        'year', 'series', 'value', 'footnotes', 'source']
        df = df[valid_columns]
        
        # Handle NaN values in footnotes
        df['footnotes'] = df['footnotes'].fillna('')
        
        # Insert into database
        with get_db() as db:
            # Clear existing data (optional)
            db.query(PopulationData).delete()
            
            # Insert new data
            records = df.to_dict('records')
            for record in records:
                db_record = PopulationData(**record)
                db.add(db_record)
            
            db.commit()
        
        return {"message": f"Successfully uploaded {len(df)} records", "records_count": len(df)}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error uploading CSV: {str(e)}")

# ============== CRUD OPERATIONS ==============

# CREATE - Create a new record
@app.post("/data/", response_model=PopulationDataResponse, status_code=201)
async def create_record(data: PopulationDataCreate):
    """
    Create a new population data record
    """
    with get_db() as db:
        db_record = PopulationData(**data.dict())
        db.add(db_record)
        db.commit()
        db.refresh(db_record)
        return db_record

# READ - Get all records with optional filters
@app.get("/data/", response_model=List[PopulationDataResponse])
async def get_data(
    region: Optional[str] = Query(None, description="Filter by region/country/area"),
    year: Optional[str] = Query(None, description="Filter by year"),
    series: Optional[str] = Query(None, description="Filter by series type"),
    limit: int = Query(100, ge=1, le=1000, description="Number of records to return"),
    offset: int = Query(0, ge=0, description="Number of records to skip")
):
    """
    Get population data with optional filters
    """
    with get_db() as db:
        query = db.query(PopulationData)
        
        if region:
            query = query.filter(PopulationData.region_country_area.ilike(f"%{region}%"))
        if year:
            query = query.filter(PopulationData.year == year)
        if series:
            query = query.filter(PopulationData.series.ilike(f"%{series}%"))
        
        results = query.offset(offset).limit(limit).all()
        return results

# READ - Get a specific record by ID
@app.get("/data/{record_id}", response_model=PopulationDataResponse)
async def get_record_by_id(record_id: int):
    """
    Get a specific record by ID
    """
    with get_db() as db:
        record = db.query(PopulationData).filter(PopulationData.id == record_id).first()
        if not record:
            raise HTTPException(status_code=404, detail="Record not found")
        return record

# UPDATE - Update a specific record
@app.put("/data/{record_id}", response_model=PopulationDataResponse)
async def update_record(record_id: int, data: PopulationDataUpdate):
    """
    Update a specific record by ID
    """
    with get_db() as db:
        record = db.query(PopulationData).filter(PopulationData.id == record_id).first()
        if not record:
            raise HTTPException(status_code=404, detail="Record not found")
        
        # Update only provided fields
        update_data = data.dict(exclude_unset=True)
        for key, value in update_data.items():
            setattr(record, key, value)
        
        db.commit()
        db.refresh(record)
        return record

# DELETE - Delete a specific record by ID
@app.delete("/data/{record_id}")
async def delete_record_by_id(record_id: int):
    """
    Delete a specific record by ID
    """
    with get_db() as db:
        record = db.query(PopulationData).filter(PopulationData.id == record_id).first()
        if not record:
            raise HTTPException(status_code=404, detail="Record not found")
        
        db.delete(record)
        db.commit()
        return {"message": f"Record {record_id} deleted successfully"}

# DELETE - Delete records based on filters
@app.delete("/data/")
async def delete_records(
    region: Optional[str] = Query(None, description="Delete by region/country/area"),
    year: Optional[str] = Query(None, description="Delete by year"),
    series: Optional[str] = Query(None, description="Delete by series type"),
    delete_all: bool = Query(False, description="Set to true to delete all records")
):
    """
    Delete records based on filters. At least one filter must be provided unless delete_all is true.
    """
    with get_db() as db:
        # If no filters provided and delete_all is not true, return error
        if not any([region, year, series, delete_all]):
            raise HTTPException(
                status_code=400, 
                detail="At least one filter parameter must be provided, or set delete_all=true"
            )
        
        query = db.query(PopulationData)
        
        # Apply filters
        if region:
            query = query.filter(PopulationData.region_country_area.ilike(f"%{region}%"))
        if year:
            query = query.filter(PopulationData.year == year)
        if series:
            query = query.filter(PopulationData.series.ilike(f"%{series}%"))
        
        # Count records before deletion
        count = query.count()
        
        if count == 0:
            return {"message": "No records found matching the criteria", "deleted_count": 0}
        
        # Delete records
        query.delete(synchronize_session=False)
        db.commit()
        
        return {
            "message": f"Successfully deleted {count} record(s)",
            "deleted_count": count
        }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
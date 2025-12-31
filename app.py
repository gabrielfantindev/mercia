from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, constr
from typing import Optional, List
import uvicorn

from dbcenter import init_pool, create_clients_table, insert_client, close_pool, get_clients

app = FastAPI(title="Mercia API")

# Allow local development origins (adjust in production)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ClientIn(BaseModel):
    name: constr(strip_whitespace=True, min_length=1)
    address: Optional[constr(strip_whitespace=True)] = None
    phone: Optional[constr(strip_whitespace=True)] = None

class ClientOut(ClientIn):
    id: int
    created_at: str


@app.on_event("startup")
async def startup_event():
    init_pool()
    # Ensure table exists
    create_clients_table()


@app.on_event("shutdown")
async def shutdown_event():
    close_pool()


@app.post("/api/clients", response_model=ClientOut)
def create_client(payload: ClientIn):
    try:
        inserted = insert_client(payload.name, payload.address, payload.phone)
        return {"id": inserted["id"], "created_at": str(inserted["created_at"]), **payload.dict()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/clients", response_model=List[ClientOut])
def list_clients(limit: int = 100):
    try:
        rows = get_clients(limit=limit)
        # normalize created_at to str
        result = []
        for r in rows:
            result.append({
                'id': r.get('id'),
                'name': r.get('name'),
                'address': r.get('address'),
                'phone': r.get('phone'),
                'created_at': str(r.get('created_at')) if r.get('created_at') is not None else None,
            })
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    # Bind to 0.0.0.0 so the API is reachable from other devices on the local network
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)

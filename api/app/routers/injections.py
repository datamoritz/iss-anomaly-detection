from fastapi import APIRouter

from ..schemas import InjectionJobCreateRequest, InjectionJobResponse
from ..services.injections import create_injection_job

router = APIRouter(tags=["injections"])


@router.post("/injections", response_model=InjectionJobResponse)
def create_injection_job_route(request: InjectionJobCreateRequest):
    return create_injection_job(request)

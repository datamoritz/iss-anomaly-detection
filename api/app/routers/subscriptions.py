from fastapi import APIRouter, Query

from ..schemas import (
    SubscriptionCreateRequest,
    SubscriptionDeleteRequest,
    SubscriptionResponse,
    SubscriptionVerifyRequest,
)
from ..services.subscriptions import (
    create_subscription,
    unsubscribe_by_id,
    unsubscribe_by_token,
    verify_subscription,
)

router = APIRouter(tags=["subscriptions"])


@router.post("/subscriptions", response_model=SubscriptionResponse)
def create_subscription_route(request: SubscriptionCreateRequest):
    subscription_id = create_subscription(request)
    return SubscriptionResponse(
        ok=True,
        message="Verification email sent",
        subscription_id=subscription_id,
    )


@router.post("/subscriptions/verify", response_model=SubscriptionResponse)
def verify_subscription_route(request: SubscriptionVerifyRequest):
    changed, subscription_id = verify_subscription(request.token)
    return SubscriptionResponse(
        ok=True,
        message="Subscription verified" if changed else "Subscription already verified",
        subscription_id=subscription_id,
    )


@router.get("/subscriptions/verify", response_model=SubscriptionResponse)
def verify_subscription_from_link(token: str = Query(...)):
    changed, subscription_id = verify_subscription(token)
    return SubscriptionResponse(
        ok=True,
        message="Subscription verified" if changed else "Subscription already verified",
        subscription_id=subscription_id,
    )


@router.post("/subscriptions/unsubscribe", response_model=SubscriptionResponse)
def unsubscribe_route(request: SubscriptionVerifyRequest):
    subscription_id = unsubscribe_by_token(request.token)
    return SubscriptionResponse(
        ok=True,
        message="Subscription disabled",
        subscription_id=subscription_id,
    )


@router.get("/subscriptions/unsubscribe", response_model=SubscriptionResponse)
def unsubscribe_from_link(token: str = Query(...)):
    subscription_id = unsubscribe_by_token(token)
    return SubscriptionResponse(
        ok=True,
        message="Subscription disabled",
        subscription_id=subscription_id,
    )


@router.delete("/subscriptions/{subscription_id}", response_model=SubscriptionResponse)
def delete_subscription_route(subscription_id: int, request: SubscriptionDeleteRequest):
    unsubscribe_by_id(subscription_id, request.token)
    return SubscriptionResponse(
        ok=True,
        message="Subscription disabled",
        subscription_id=subscription_id,
    )

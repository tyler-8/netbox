import threading
from contextlib import contextmanager

from django.db.models.signals import m2m_changed, pre_delete, post_save

from extras.signals import clear_webhooks, _clear_webhook_queue, _handle_changed_object, _handle_deleted_object
from utilities.utils import curry
from .webhooks import flush_webhooks

changelog_lock = threading.Lock()


@contextmanager
def change_logging(request):
    """
    Enable change logging by connecting the appropriate signals to their receivers before code is run, and
    disconnecting them afterward.

    :param request: WSGIRequest object with a unique `id` set
    """

    webhook_queue = []

    # Curry signals receivers to pass the current request
    handle_changed_object = curry(_handle_changed_object, request, webhook_queue)
    handle_deleted_object = curry(_handle_deleted_object, request, webhook_queue)
    clear_webhook_queue = curry(_clear_webhook_queue, webhook_queue)

    # Connect our receivers to the post_save and post_delete signals.
    post_save.connect(handle_changed_object, dispatch_uid=f'handle_changed_object_{request.id}')
    m2m_changed.connect(handle_changed_object, dispatch_uid=f'handle_changed_object_{request.id}')
    pre_delete.connect(handle_deleted_object, dispatch_uid=f'handle_deleted_object_{request.id}')
    clear_webhooks.connect(clear_webhook_queue, dispatch_uid=f'clear_webhook_queue_{request.id}')

    yield

    # Disconnect change logging signals. This is necessary to avoid recording any errant
    # changes during test cleanup.
    post_save.disconnect(handle_changed_object, dispatch_uid=f'handle_changed_object_{request.id}')
    m2m_changed.disconnect(handle_changed_object, dispatch_uid=f'handle_changed_object_{request.id}')
    pre_delete.disconnect(handle_deleted_object, dispatch_uid=f'handle_deleted_object_{request.id}')
    clear_webhooks.disconnect(clear_webhook_queue, dispatch_uid=f'clear_webhook_queue_{request.id}')

    # Flush queued webhooks to RQ
    flush_webhooks(webhook_queue)
    del webhook_queue

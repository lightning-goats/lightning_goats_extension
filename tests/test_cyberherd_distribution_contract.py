"""Feeder success must authorize the distribution rather than delegate blindly."""
import asyncio
from types import SimpleNamespace
from lnbits.extensions.lightning_goats import tasks
from lnbits.extensions.cyberherd import crud
from lnbits.extensions.cyberherd.services import send_splits

def test_auto_distribution_uses_successful_feeder_context(monkeypatch):
 config=SimpleNamespace(send_splits_enabled=True)
 calls=[]
 async def settings(owner):return config
 async def distribute(owner,settings=None,*,feeder_triggered=False):
  calls.append((owner,settings,feeder_triggered))
  return {'ok':True,'sats':1000}
 monkeypatch.setattr(crud,'get_settings',settings)
 monkeypatch.setattr(send_splits,'maybe_send_splits_for_user',distribute)
 asyncio.run(tasks._distribute_herd_balance('owner'))
 assert calls==[('owner',config,True)]

def test_failed_distribution_is_not_reported_done(monkeypatch):
 import pytest
 config=SimpleNamespace(send_splits_enabled=True)
 async def settings(owner):return config
 async def distribute(*args,**kwargs):return {'ok':False,'error':'invoice failed'}
 monkeypatch.setattr(crud,'get_settings',settings)
 monkeypatch.setattr(send_splits,'maybe_send_splits_for_user',distribute)
 with pytest.raises(RuntimeError,match='invoice failed'):
  asyncio.run(tasks._distribute_herd_balance('owner'))

def test_stop_unregisters_current_core_listener(monkeypatch):
 from lnbits.tasks import task_manager
 cancelled=[]
 listener=SimpleNamespace(name='forward_'+tasks.INVOICE_LISTENER_NAME+'_invoice_listener',task=SimpleNamespace(cancel=lambda:cancelled.append(True)))
 monkeypatch.setattr(task_manager,'tasks',[listener])
 monkeypatch.setattr(tasks,'scheduled_tasks',[])
 asyncio.run(tasks.stop_background_tasks())
 assert task_manager.tasks==[]
 assert cancelled==[True]

def test_triggering_payment_and_targets_are_ready_before_transfer(monkeypatch):
 from lnbits.extensions.cyberherd.services import payment_coordinator,splits
 config=SimpleNamespace(send_splits_enabled=True,source_wallet='source')
 ordered=[];payment=object()
 async def settings(owner):return config
 async def sync(incoming,settings=None):
  assert incoming is payment;ordered.append('sync')
 async def targets(source,user_id=None,*,strict=False):
  assert strict is True;ordered.append('targets')
 async def distribute(*args,**kwargs):ordered.append('transfer');return {'ok':True}
 monkeypatch.setattr(crud,'get_settings',settings)
 monkeypatch.setattr(payment_coordinator,'get_payment_coordinator',lambda **k:SimpleNamespace(sync_payment_for_distribution=sync))
 monkeypatch.setattr(splits,'update_split_targets_proportional',targets)
 monkeypatch.setattr(send_splits,'maybe_send_splits_for_user',distribute)
 asyncio.run(tasks._distribute_herd_balance('owner',payment=payment))
 assert ordered==['sync','targets','transfer']

def test_retryable_intake_failure_prevents_distribution(monkeypatch):
 import pytest
 from lnbits.extensions.cyberherd.services import payment_coordinator
 config=SimpleNamespace(send_splits_enabled=True,source_wallet='source')
 async def settings(owner):return config
 async def sync(*a,**k):raise RuntimeError('retryable zap metadata lookup')
 async def distribute(*a,**k):raise AssertionError('must not transfer')
 monkeypatch.setattr(crud,'get_settings',settings)
 monkeypatch.setattr(payment_coordinator,'get_payment_coordinator',lambda **k:SimpleNamespace(sync_payment_for_distribution=sync))
 monkeypatch.setattr(send_splits,'maybe_send_splits_for_user',distribute)
 with pytest.raises(RuntimeError,match='retryable zap metadata lookup'):
  asyncio.run(tasks._distribute_herd_balance('owner',payment=object()))

def test_failed_target_refresh_prevents_transfer(monkeypatch):
 import pytest
 from lnbits.extensions.cyberherd.services import splits
 config=SimpleNamespace(send_splits_enabled=True,source_wallet='source')
 async def settings(owner):return config
 async def refresh(*args,**kwargs):raise RuntimeError('target storage unavailable')
 async def distribute(*a,**k):raise AssertionError('must not transfer')
 monkeypatch.setattr(crud,'get_settings',settings)
 monkeypatch.setattr(splits,'update_split_targets_proportional',refresh)
 monkeypatch.setattr(send_splits,'maybe_send_splits_for_user',distribute)
 with pytest.raises(RuntimeError,match='target storage unavailable'):
  asyncio.run(tasks._distribute_herd_balance('owner'))

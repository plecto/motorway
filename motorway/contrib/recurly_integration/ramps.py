import os
import time

from motorway.messages import Message
from motorway.ramp import Ramp

"""
Requires pip install recurly
"""


class RecurlyRamp(Ramp):
    @property
    def recurly(self):
        import recurly
        recurly.SUBDOMAIN = os.environ.get('RECURLY_SUBDOMAIN')
        recurly.API_KEY = os.environ.get('RECURLY_API_KEY')
        return recurly


class RecurlyInvoiceRamp(RecurlyRamp):

    def next(self):
        for invoice in self.recurly.Invoice.all():
            yield Message(invoice.uuid, {
                'uuid': invoice.uuid,
                'invoice_number': invoice.invoice_number,
                'vat_number': invoice.vat_number,
                'total_in_cents': invoice.total_in_cents,
                'tax_in_cents': invoice.tax_in_cents,
                'subtotal_in_cents': invoice.subtotal_in_cents,
                'state': invoice.state,
                'collection_method': invoice.collection_method,
                'currency': invoice.currency,
                'account': invoice.account().account_code,  # TODO: Parse this from the href in the XML
                'created_at': invoice.created_at,
                'updated_at': invoice.updated_at,
                'closed_at': invoice.closed_at,
            })
        time.sleep(3600)


class RecurlyAccountRamp(RecurlyRamp):

    def next(self):
        for account in self.recurly.Account.all():
            yield Message(account.account_code, {
                'company_name': account.company_name,
                'account_code': account.account_code,
            })
        time.sleep(3600)

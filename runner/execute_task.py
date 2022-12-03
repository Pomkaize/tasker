import aiohttp
import pytz
from datetime import datetime
from dateutil import parser
from dateutil.relativedelta import relativedelta

from .vars import REALTY_API_HOST, REALTY_API_TOKEN


def get_rent_user_request_url(user_id):
    return f"{REALTY_API_HOST}/2.0/rent/moderation/users/{user_id}"


def get_rent_user_spectrum_report_url(user_id, report_id):
    return f"{REALTY_API_HOST}/2.0/rent/moderation/users/{user_id}/report/{report_id}"


def get_user_age(user_data):
    now = datetime.now(pytz.utc)
    birthday_string = user_data.get('user', {}).get('passportData', {}).get('birthday', '')
    age_string = ''

    if birthday_string != '':
        birthday_datetime = parser.parse(birthday_string)
        age_string = str(relativedelta(now, birthday_datetime).years)

    return age_string


async def execute_task(user_id):
    async with aiohttp.ClientSession() as session:
        request_user_url = get_rent_user_request_url(user_id)
        request_user_params = {
            'withPersonalData': 'true'
        }
        headers = {
            'X-Authorization': REALTY_API_TOKEN,
            'X-Uid': '123'
        }

        async with session.get(request_user_url, params=request_user_params, headers=headers) as user_response:
            if user_response.status != 200:
                return {
                    'status_code': user_response.status,
                    'response': await user_response.text()
                }

            response = await user_response.json()
            user = response.get('response', {}).get('user')

        checks = user.get('naturalPersonChecks', {}).get('checks', [])

        fssp_debt_check_list = list(filter(lambda ch: ch.get('fsspDebtCheck', {}) != {}, checks))
        report_response = {}

        if fssp_debt_check_list:
            fssp_debt_check = fssp_debt_check_list[0].get('fsspDebtCheck', {})
            palma_report_id = fssp_debt_check.get('palmaReportId', '')
            request_user_spectrum_report_url = get_rent_user_spectrum_report_url(user_id, palma_report_id)

            async with session.get(request_user_spectrum_report_url, headers=headers) as user_report_response:
                if user_report_response.status == 200:
                    response = await user_report_response.json()

                    report_response = response.get('response', {})

        reports = report_response.get('proceedingExecutiveReport', {}).get('items', [])
        debt_balances = map(lambda d: float(d.get('debtBalancePrincipal', '0')), reports)

        max_debt_balance = max(debt_balances, default=0)
        user_age = get_user_age(user)

    return {
        'user_id': user_id,
        'user_age': user_age,
        'max_debt_balance': max_debt_balance,
    }

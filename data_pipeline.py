import ast
import argparse
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions


def run(argv=None):
    parser =  argparse.ArgumentParser()
    parser.add_argument('-l', '--local', dest='local', type=bool, default=True, help='If passed then code will'
                                                                                      'run on local system')
    known_args, pipeline_args = parser.parse_known_args(argv)
    print(known_args)
    pipeline_args.append('--project=fast-ai-vision')

    # If not running on a local system, i.e. deploying on dataflow
    if not known_args.local:
        print('Entered')
        pipeline_args.extend([
            '--runner=DataFlowRunner',
            '--staging_location=gs://ri_assignment/staging',
            '--temp_location=gs://ri_assignment/tmp',
            '--job_name=ri-assignment',
            '--region=us-east1'
        ])

    options = PipelineOptions(pipeline_args)
    options.view_as(SetupOptions).save_main_session = True


    class RemoveUnwantedCompanies(beam.DoFn):
        def process(self, element, *args, **kwargs):
            from datetime import datetime, timedelta

            # remove non-operational companies
            year_of_import = (datetime.strptime(element.get('import_datetime'), '%Y-%m-%d %H:%M:%S.%f UTC') -
                              timedelta(days= 365 * 3)).year

            # The companies that have status as operational and have a year of import - 3 > launch year
            # are the ones that are allowed in the final table
            if element.get('company_status') == 'operational' and year_of_import > element.get('launch_year'):
                element['launch_year'] = int(element.get('launch_year'))
                return [element]


    # class for team size calculation
    class TeamSizeCalc(beam.DoFn):
        def process(self, element, *args, **kwargs):
            team_size = len(ast.literal_eval(element.get('team', {})).get('items'))
            return [{**element, **{'team_size': team_size}}]


    class Employees3YearsAgo(beam.DoFn):
        def process(self, element, *args, **kwargs):
            from datetime import datetime, timedelta

            date_of_import = (datetime.strptime(element.get('import_datetime'), '%Y-%m-%d %H:%M:%S.%f UTC') -
                              timedelta(days= 365 * 3)).date()

            emp_chart = ast.literal_eval(element.get('employees_chart', []))
            # expect an empty list instead of None, and allow entry of only values exist
            min_date, emp_value = datetime(year=1970, month=1, day=1).date(), 0

            if emp_chart:
                for emp in emp_chart:
                    # here the date is str, cast to datetime.date
                    date = datetime.strptime(emp.get('date'), '%Y-%m-%d').date()
                    val = int(emp.get('value'))

                    if date_of_import > date > min_date:
                        min_date = date
                        emp_value = val
            element['employees_3y_ago'] = emp_value if min_date > datetime(year=1970, month=1, day=1).date() else None
            return [element]


    with beam.Pipeline(options=options) as p:
        (
            # Each element is passed as a dict
            p   | "Read BQ" >> beam.io.Read(beam.io.BigQuerySource(dataset='ri_assignment_piyush',
                                                                 table='in_company_2', use_standard_sql=True))
                | "Remove Unnecessary Companies" >> beam.ParDo(RemoveUnwantedCompanies())
                | "Team Size" >> beam.ParDo(TeamSizeCalc())
                | "Employees 3 Years Ago" >> beam.ParDo(Employees3YearsAgo())
                | "Dump to BigQuery" >> beam.io.WriteToBigQuery(table='result_from_dataflow', dataset='ri_assignment_piyush')
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
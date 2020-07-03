import argparse
from p_acquisition import m_acquisition as mac
from p_wrangling import m_wrangling as mwr
from p_analysis import m_analysis as man

def argument_parser():
    parser = argparse.ArgumentParser(description='Select a country...')
    parser.add_argument("-c", "--country", type=str, dest='country', help='Please select the country to start the analysis...')
    parser.add_argument("-p", "--path", type=str, help='Please state the path you wish to analyse', required=True)
    args = parser.parse_args()
    return args

def main(args):
    print('Starting pipeline and retrieving information...')
    print('Getting information from database analysed...')
    df_m1 = mac.acquire(args.path)
    df_m2 = mwr.wrangling(df_m1)

    if args.country is None:
        print('retrieving information for all countries...')
        df_m3 = man.analysis(df_m2, 'all')

    else:
        df_m3 = man.analysis(df_m2, args.country)

    print('********************* Pipeline is complete, you can find the results in the data results folder *********************')

if __name__ == '__main__':
    arguments = argument_parser()
    main(arguments)



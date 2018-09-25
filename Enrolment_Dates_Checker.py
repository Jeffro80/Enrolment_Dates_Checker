# Enrolment Dates Checker
# Version 1.0 25 September 2018
# Created by Jeff Mitchell
# Processing Enrolment Dates in the Learning Platform

# Data sources:

# Enrolment Dates query from Learning Platform
# Enrolments Table in Student Database

# To Do:

# 
import csv
import custtools.admintools as ad
import custtools.databasetools as db
import custtools.datetools as da
import custtools.filetools as ft
import sys
import pandas as pd


def check_edsd(report_data):
    """Check the data in the Enrolment Dates (Student Database) data file.
    
    Checks the Enrolment Dates (Student Database) report data to see if the 
    required information is present. Missing or incorrect information that is 
    non-fatal is appended to a warnings list and returned.

    Args:
        report_data (list): Enrolment Dates (Student Database) data file

    Returns:
        True if warnings list has had items appended to it, False otherwise.
        warnings (list): Warnings that have been identified in the data.

    File Structure (Enrolment Dates (Student Database) data file):
        Student ID, Course, Tutor, Enrolment Date, Expiry Date.
        
    File Source(Enrolment Dates (Student Database) data file):
        qryEnrolmentDates in the Student Database.
    """
    errors = []
    warnings = ['\nEnrolment Dates (Student Database) Report Warnings:\n']
    for student in report_data:
        if student[1] in (None, ''):
            errors.append('Course code is missing for student with '
                            'Student ID {}'.format(student[0]))
        if student[2] in (None, ''):
            warnings.append('Tutor is missing for student with Student ID'
                          ' {}'.format(student[0]))
        if student[3] in (None, ''):
            errors.append('Enrolment Date is missing for student with Student '
                            'ID {}'.format(student[0]))
        if student[4] in (None, ''):
            errors.append('Expiry Date is missing for student with Student '
                            'ID {}'.format(student[0]))
    # Check if any errors have been identified, save error log if they have
    if len(errors) > 0:
        ft.process_error_log(errors,
                             'Enrolment Dates (Student Database) Report')
    # Check if any warnings have been identified, save error log if they have
    if len(warnings) > 1:
        return True, warnings
    else:
        return False, warnings


def check_edlp(report_data):
    """Check the data in the Enrolment Dates (Learning Platform) data file.
    
    Checks the Enrolment Dates (Learning Platform) report data to see if the 
    required information is present. Missing or incorrect information that is 
    non-fatal is appended to a warnings list and returned.

    Args:
        report_data (list): Enrolment Dates (Learning Platform) data file

    Returns:
        True if warnings list has had items appended to it, False otherwise.
        warnings (list): Warnings that have been identified in the data.

    File Structure (Enrolment Dates (Learning Platform) data file):
        Student ID, Student, Course, Enrolment Date, Expiry Date
        
    File Source (Enrolment Dates (Learning Platform) data file):
        Enrolment Dates database query in the Learning Platform.
    """
    errors = []
    warnings = ['\nEnrolment Dates (Learning Platform) Report Warnings:\n']
    for student in report_data:
        if student[1] in (None, ''):
            warnings.append('Student Name is missing for student with '
                            'Student ID {}'.format(student[0]))
        if student[2] in (None, ''):
            errors.append('Course is missing for student with Student ID'
                          ' {}'.format(student[0]))
        if student[3] in (None, ''):
            errors.append('Enrolment Date is missing for student with Student '
                            'ID {}'.format(student[0]))
        if student[4] in (None, ''):
            errors.append('Expiry Date is missing for student with Student '
                            'ID {}'.format(student[0]))
    # Check if any errors have been identified, save error log if they have
    if len(errors) > 0:
        ft.process_error_log(errors,
                             'Enrolment Dates (Learning Platform) Report')
    # Check if any warnings have been identified, save error log if they have
    if len(warnings) > 1:
        return True, warnings
    else:
        return False, warnings


def clean_date(date):
    """Return date in DD/MM/YYYY.
    
    If the date has only 1 digit for DD, a leading 0 is added.
    
    Args:
        date (str): Date to be checked.
    
    Return:
        date with a leading 0 if one is missing, else date.
    """
    if len(date) == 9:
        return ('0{}'.format(date))
    else:
        return date


def clean_date_2(date):
    """Convert a datetime object to a string in format DD/MM/YYYY.
    
    Args:
        date (datetime): Date in format YYYY-MM-DD.
        
    Returns:
        cleaned_date (str): Date in format DD/MM/YYYY
    """
    # print('Debugging: {}'.format(date))
    str_date = str(date)
    day = str_date[8:10]
    month = str_date[5:7]
    year = str_date[:4]
    cleaned_date = day + '/' + month + '/' + year
    return cleaned_date


def clean_lp_data(raw_data):
    """Extract and clean desired Learning Platform data.
    
    Cleans the data in the Learning Platform data so that it is in the correct
    format and returns the desired columns.
    
    Args:
        raw_data (list) Student data.
        
    Returns:
        cleaned_data (list): Cleaned student data.
    
    File Structure (raw_data):
        Student ID, Student, Course, Enrolment Date, Expiry Date
        
    File Source(raw_data):
        Enrolment Dates database query in the Learning Platform.
    """
    cleaned_data = []
    for student in raw_data:
        cleaned_student = []
        cleaned_student.append(student[0].strip())
        cleaned_student.append(student[1].strip())
        # Extract Course code
        cleaned_student.append(db.extract_course_code(student[2]).strip())
        # Extract dates - update
        cleaned_student.append(da.clean_date(student[3]))
        cleaned_student.append(da.clean_date(student[4]))
        cleaned_data.append(cleaned_student)
    # Remove students with 'Skip' in the course column
    cleaned_data = remove_students(cleaned_data, 2, 'Skip')
    return cleaned_data


def clean_sd_data(raw_data):
    """Extract and clean desired Student Database data.
    
    Cleans the data in the Student Database data so that it is in the correct
    format and returns the desired columns.
    
    Args:
        raw_data (list) Student data.
        
    Returns:
        clean_data (list): Cleaned student data.
    
    File Structure (Enrolment Dates (Student Database) data file):
        Student ID, Course, Tutor, Enrolment Date, Expiry Date
        
    File Source(Enrolment Dates (Student Database) data file):
        Enrolments table in the Student Database.
    """
    # Work through each student, extracting one at a time to process
    cleaned_data = []
    for student in raw_data:
        cleaned_student = []
        cleaned_student.append(student[0].strip())
        cleaned_student.append(student[1].strip())
        cleaned_student.append(clean_date(student[3]))
        cleaned_student.append(clean_date(student[4]))
        cleaned_data.append(cleaned_student)
    return cleaned_data


def compare_dates(df_1, df_2, common, col_name, headings):
    """Return students with different values in specified columns.
    
    Merges the Enrolments DataFrame and Database DataFrame. Compares the
    specified date columns and records students where the dates differ.
    Places the students into a DataFrame.
    
    Args:
        df_1 (DataFrame): Enrolments DataFrame.
        df_2 (DataFrame): Database DataFrame.
        common (str): Name of common to join on (with StudentID).
        col_name (str): Name of column to compare on.
        headings (list): Column names for output DataFrame
    
    Returns:
        identified (DataFrame): Students with differing dates.
    """
    combined = pd.merge(df_1, df_2, on = common, how = 'left', suffixes =
                        ['_1', '_2'])
    # Create a list of students with different dates
    differing = []
    for index, row in combined.iterrows():
        if row[col_name + '_1'] != row[col_name + '_2']:
            differing.append(row[headings])
    # Create a DataFrame to be returned of the relevant students
    identified = pd.DataFrame(data = differing, columns = headings)
    return identified


def load_data(file_name, source):
    """Read data from a file.

    Args:
        file_name (str): The name of the file to be read.
        source (str): The code for the table that the source data belongs to.

    Returns:
        read_data (list): A list containing the data read from the file.
        True if warnings list has had items appended to it, False otherwise.
        warnings (list): Warnings that have been identified in the data.
    """
    read_data = []
    warnings = []
    # print('File name = ' + str(file_name))
    # Check that file exists
    valid_file = False
    while not valid_file:
        try:
            file = open(file_name + '.csv', 'r')
        except IOError:
            print('The file {}.csv does not exist. Please check the file name '
                  'before trying again.'.format(file_name))
            file_name = input('What is the name of the file? ')
        else:
            file.readline()
            reader = csv.reader(file, delimiter=',', quotechar='"')
            for row in reader:
                if row[0] not in (None, ''):
                    read_data.append(row)
            file.close()
            # Check that data has entries for each required column
            if source == 'edlp':
                check_edlp(read_data)
            elif source == 'edsd':
                check_edsd(read_data)
            valid_file = True
    if len(warnings) > 0:
        return read_data, True, warnings
    else:
        return read_data, False, warnings


def main():
    repeat = True
    low = 1
    high = 3
    while repeat:
        try_again = False
        main_message()
        try:
            action = int(input('\nPlease enter the number for your '
                               'selection --> '))
        except ValueError:
            print('Please enter a number between {} and {}.'.format(low, high))
            try_again = True
        else:
            if int(action) < low or int(action) > high:
                print('\nPlease select from the available options ({} - {})'
                      .format(low, high))
                try_again = True
            elif action == low:
                process_enrolment_dates()
            elif action == 2:
                process_enrolments()
            elif action == high:
                print('\nIf you have generated any files, please find them '
                      'saved to disk. Goodbye.')
                sys.exit()
        if not try_again:
            repeat = ad.check_repeat()
    print('\nPlease find your files saved to disk. Goodbye.')


def main_message(): # UPDATE
    """Print the menu of options."""
    print('\n\n*************==========================*****************')
    print('\nEnrolments Dates Checker version 1.0')
    print('Created by Jeff Mitchell, 2018')
    print('\nOptions:')
    print('\n1 Enrolments Date Comparison')
    print('2 Clean Enrolment Dates')
    print('3 Exit')


def process_enrolments():
    """Clean list of student enrolment dates.
    
    Cleans the list of student enrolment dates and returns only those for
    courses, removing Navigation course and internal courses. Courses kept
    have the Course Code XXX-XX-XXX.
    """
    warnings = ['\nProcessing Enrolments data Warnings:\n']
    warnings_to_process = False
    print('\nEnrolments data.')
    # Confirm the required files are in place
    required_files = ['Enrolments (Learning Platform)']
    ad.confirm_files('Enrolments Report', required_files)
    # Load file
    lp_file_name = input('\nWhat is the name of the Enrolments (Learning '
                         'Platform) file? --> ')
    print('\nLoading {}...'.format(lp_file_name))
    raw_lp_data, to_add, warnings_to_add = load_data(lp_file_name, 'edlp')
    print('\nLoaded {}.'.format(lp_file_name))
    # Clean and extract desired data
    lp_data = clean_lp_data(raw_lp_data)
    # Variables for column names
    sid_name = 'StudentID'
    s_name = 'Student'
    c_name = 'Course'
    sd_name = 'Start Date'
    ex_name = 'Expiry Date'
    # Place in DataFrame
    headings = [sid_name, s_name, c_name, sd_name, ex_name]
    lp_df = pd.DataFrame(data = lp_data, columns = headings)
    # Order on Enrolment Date
    lp_df[sd_name] = pd.to_datetime(lp_df[sd_name], format = "%d/%m/%Y")
    lp_df = lp_df.sort_values([sd_name])
    # Convert Date Column to string DD/MM/YYYY
    lp_df[sd_name] = lp_df[sd_name].apply(clean_date_2)
    # Save DataFrame to file
    f_name = 'Enrolment_Dates_{}.xls'.format(ft.generate_time_string())
    lp_df.to_excel(f_name, index=False)
    print('\nEnrolment_Dates_ has been saved to {}'.format(f_name))
    ft.process_warning_log(warnings, warnings_to_process)


def process_enrolment_dates():
    """Find Enrolment and Expiry Dates that do not match.
    
    Compares the data from the Learning Platform and the Student Database to
    find students whose Start Date or Expiry Date does not match between the
    two.
    """
    warnings = ['\nProcessing Enrolment Dates data Warnings:\n']
    warnings_to_process = False
    print('\nEnrolment Dates data.')
    # Confirm the required files are in place
    required_files = ['Enrolment Dates (Learning Platform)', 'Enrolment Dates '
                      '(Student Databse)']
    ad.confirm_files('Enrolment Dates Report', required_files)
    # Load Enrolment Dates (Learning Platform)
    lp_file_name = input('\nWhat is the name of the Enrolment Dates (Learning '
                         'Platform) file? --> ')
    print('\nLoading {}...'.format(lp_file_name))
    raw_lp_data, to_add, warnings_to_add = load_data(lp_file_name, 'edlp')
    print('\nLoaded {}.'.format(lp_file_name))
    # Clean and extract desired data
    lp_data = clean_lp_data(raw_lp_data)
    # Variables for column names
    sid_name = 'StudentID'
    s_name = 'Student'
    c_name = 'Course'
    sd_name = 'Start Date'
    ex_name = 'Expiry Date'
    # Create a DataFrame with Learning Platform data
    headings = [sid_name, s_name, c_name, sd_name, ex_name]
    lp_df = pd.DataFrame(data = lp_data, columns = headings)
    # Load Enrolment Dates (Student Database)
    sd_file_name = input('\nWhat is the name of the Enrolment Dates (Student '
                         'Database) file? --> ')
    print('\nLoading {}...'.format(sd_file_name))
    raw_sd_data, to_add, warnings_to_add = load_data(sd_file_name, 'edsd')
    print('\nLoaded {}.'.format(sd_file_name))
    # Clean and extract desired data
    sd_data = clean_sd_data(raw_sd_data)
    # Create a DataFrame with Student Database data
    headings = [sid_name, c_name, sd_name, ex_name]
    sd_df = pd.DataFrame(data = sd_data, columns = headings)
    # print('Students in Enrolments: {}'.format(len(lp_df.index)))
    # print('Students in Database: {}'.format(len(sd_df.index)))
    # Find missing students
    # Compare two DataFrames and find students with differing Start Dates
    headings = [sid_name, s_name, 'Course_1', 'Start Date_1', 'Start Date_2']
    start = compare_dates(lp_df, sd_df, sid_name, sd_name, headings)
    start = start.rename(columns={'Start Date_1':'Start Date LP',
                                  'Start Date_2':'Start Date SD'})
    # Save Master file
    f_name = 'Start_Dates_{}.xls'.format(ft.generate_time_string())
    start.to_excel(f_name, index=False)
    print('\nStart_Dates_{} has been saved to '.format(f_name))
    # Compare two DataFrames and find students with differing Start Dates
    headings = [sid_name, s_name, 'Course_1', 'Expiry Date_1', 'Expiry Date_2']
    expiry = compare_dates(lp_df, sd_df, sid_name, ex_name, headings)
    expiry = expiry.rename(columns={'Expiry Date_1':'Expiry Date LP',
                                  'Expiry Date_2':'Expiry Date SD'})
    # Save Master file
    f_name = 'Expiry_Dates_{}.xls'.format(ft.generate_time_string())
    expiry.to_excel(f_name, index=False)
    print('\nExpiry_Dates_ has been saved to {}'.format(f_name))
    ft.process_warning_log(warnings, warnings_to_process)


def remove_students(students, location, identifier):
    """Remove students from list based on identifier.
    
    Removes students from the list if the identified column contains the
    identifier. Returns a list minus these students.
    
    Args:
        students (list): Data for the students.
        location (int): The column to look for the identifier in.
        identifier (str): The string to look for - if found, the student is
        removed from the list.
    
    Returns:
        updated_list (list): List with the identified students removed.
    """
    updated_list = []
    for student in students:
        if student[location] != identifier:
            updated_list.append(student)
    return updated_list


if __name__ == '__main__':
    main()
"""
This is used for testing basic functionality of the test client.
To run change the desired flags below and use the following command from the tap-tester repo:
  'python ../tap-adroll/tests/client_tester.py'
"""
from test_client import TestClient

##########################################################################
# Testing the TestCLient
##########################################################################
if __name__ == "__main__":
    client = TestClient()

    # CHANGE FLAGS HERE TO TEST SPECIFIC FUNCTION TYPES
    test_creates = False
    test_updates = True
    test_gets = False
    test_deletes = False

    # CHANGE FLAG TO PRINT ALL OBJECTS THAT FUNCTIONS INTERACT WITH
    print_objects = True

    objects_to_test = [ # CHANGE TO TEST DESIRED STREAMS 
        'campaigns', # GET - DONE | CREATE - DONE (werid  RATE LIMIT) | UPDATES - DONE
    ]
    # 'ads', # GET - DONE | CREATE - DONE | UPDATE - DONE
    # 'ad_groups', # GET - DONE | CREATE - DONE | UPDATE - DONE
    # 'segments', # GET - DONE | CREATE - INPROGRESS need to implement audience endpoint jawn
    # 'advertisables', # GET - DONE | CREATE NA (DONT DO THIS ONE)
    # 'ad_reports', GET - DONE | CREATE - N/A


    print("********** Testing basic functions of test client **********")
    if test_gets:
        for obj in objects_to_test:
            print("Testing GET (all): {}".format(obj))
            # import pdb; pdb.set_trace() # UNCOMMENT TO RUN 'INTERACTIVELY'
            existing_obj = client.get_all(obj)
            if existing_obj:
                print("SUCCESS")
                if print_objects:
                    import pdb; pdb.set_trace()
                    print(existing_obj)
                continue
            print("FAILED")
    if test_creates:
        for obj in objects_to_test:
            print("Testing CREATE: {}".format(obj))
            # import pdb; pdb.set_trace() # UNCOMMENT TO RUN 'INTERACTIVELY'
            created_obj = client.create(obj)
            if created_obj:
                print("SUCCESS")
                if print_objects:
                    print(created_obj)
                continue
            print("FAILED")
    if test_updates:
        for obj in objects_to_test:
            print("Testing UPDATE: {}".format(obj))
            # import pdb; pdb.set_trace() # UNCOMMENT TO RUN 'INTERACTIVELY'
            updated_obj = client.update(obj)
            if updated_obj:
                print("SUCCESS")
                if print_objects:
                    print(updated_obj)
                continue
            print("FAILED")

# DataOrganizer
Small Java based project to arrange an unorganized data into the specified structure based on the details provided in the excel file.

###  Prerequisites:
 - Java to run the executable. Java version must be >= 8.
 - Enough permission on the system where script is supposed to be run.
 - Source folder must have at-least read permission.
 - Target folder must have read and write permission.
 - Write permission on user's home drive.
 - Executable permission on the java executable script.


### *Example* ###
**Raw data format in INPUT FOLDER** 

    Root-
        -cpb-1
              - h264 
                    - cpb-1.h264.mov
        -cpb-2
              - mpeg2
                    - cpb-1.mpeg2.mxf


At the target folder location, script will create the folder structure based on the values in the excel file and copy the files their.

### How to Start:
    - Run Java -jar ArrangeData.jar help
    - Follow the instructions and create file organizer.properties at location
        {user.home}\temp\data-organizer\resources.
    - Now run java -jar ArrangeData.jar
    - For progress, follow the log file app.log at {$user.home}\temp\data-organizer\logs.
   
    *NOTE - If any folder which is already copied or in-progress and
            you want to restart from begining then delete entry with 
            key START_INDEX_{SOURCE_PATH}

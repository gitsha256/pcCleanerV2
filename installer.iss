[Setup]
AppName=PC Cleaner
AppVersion=1.0
DefaultDirName={autopf}\PC Cleaner
DefaultGroupName=PC Cleaner
OutputBaseFilename=PC_Cleaner_Installer
Compression=lzma
SolidCompression=yes
DisableProgramGroupPage=yes
UninstallDisplayIcon={app}\pcCleaner.exe
ArchitecturesInstallIn64BitMode=x64

[Files]
Source: "dist\pcCleaner.exe"; DestDir: "{app}"; Flags: ignoreversion

[Icons]
Name: "{group}\PC Cleaner"; Filename: "{app}\pcCleaner.exe"
Name: "{userdesktop}\PC Cleaner"; Filename: "{app}\pcCleaner.exe"; Tasks: desktopicon
Name: "{group}\Uninstall PC Cleaner"; Filename: "{uninstallexe}"

[Tasks]
Name: "desktopicon"; Description: "Create a &desktop shortcut"; GroupDescription: "Additional icons:"; Flags: checkedonce

[Run]
Filename: "{app}\pcCleaner.exe"; Description: "Launch PC Cleaner"; Flags: nowait postinstall skipifsilent

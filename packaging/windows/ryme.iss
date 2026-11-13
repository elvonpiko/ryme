#ifndef MyAppVersion
  #define MyAppVersion "0.1.1"
#endif

[Setup]
AppId={{7C4E9A2B-1F6D-4E8A-9C3B-5D2A8F0E6B14}
AppName=Ryme
AppVersion={#MyAppVersion}
AppPublisher=Ryme
AppPublisherURL=https://ryme.chat
AppSupportURL=https://ryme.chat/docs/clients
AppUpdatesURL=https://ryme.chat
VersionInfoVersion={#MyAppVersion}.0
VersionInfoProductName=Ryme
DefaultDirName={autopf}\Ryme
DefaultGroupName=Ryme
DisableProgramGroupPage=yes
OutputDir=out
OutputBaseFilename=Ryme-Setup-x64
SetupIconFile=..\icon\ryme.ico
UninstallDisplayIcon={app}\ryme.exe
UninstallDisplayName=Ryme
Compression=lzma2
SolidCompression=yes
WizardStyle=modern
PrivilegesRequired=admin
ArchitecturesAllowed=x64compatible
ArchitecturesInstallIn64BitMode=x64compatible
MinVersion=10.0
ChangesEnvironment=yes
CloseApplications=yes

[Tasks]
Name: "desktopicon"; Description: "Create a desktop shortcut"; GroupDescription: "Shortcuts:"; Flags: checkedonce
Name: "addpath"; Description: "Add Ryme to the PATH"; GroupDescription: "Other:"; Flags: checkedonce

[Files]
Source: "stage\ryme.exe"; DestDir: "{app}"; DestName: "ryme.exe"; Flags: ignoreversion
Source: "stage\vc_redist.x64.exe"; DestDir: "{tmp}"; Flags: deleteafterinstall

[Icons]
Name: "{group}\Ryme"; Filename: "{app}\ryme.exe"
Name: "{commondesktop}\Ryme"; Filename: "{app}\ryme.exe"; Tasks: desktopicon

[Registry]
Root: HKLM; Subkey: "SYSTEM\CurrentControlSet\Control\Session Manager\Environment"; ValueType: expandsz; ValueName: "Path"; ValueData: "{code:SystemPath}"; Tasks: addpath
Root: HKLM; Subkey: "Software\Classes\ryme"; ValueType: string; ValueName: ""; ValueData: "URL:Ryme invite"; Flags: uninsdeletekey
Root: HKLM; Subkey: "Software\Classes\ryme"; ValueType: string; ValueName: "URL Protocol"; ValueData: ""
Root: HKLM; Subkey: "Software\Classes\ryme\shell\open\command"; ValueType: string; ValueName: ""; ValueData: """{app}\ryme.exe"" ""%1"""

[Run]
Filename: "{tmp}\vc_redist.x64.exe"; Parameters: "/install /quiet /norestart"; StatusMsg: "Installing the Microsoft Visual C++ runtime..."; Flags: waituntilterminated; Check: not VCRedistInstalled
Filename: "{app}\ryme.exe"; Description: "Open Ryme"; Flags: nowait postinstall skipifsilent
Filename: "{app}\ryme.exe"; Flags: nowait; Check: WizardSilent

[Code]
const
  EnvKey = 'SYSTEM\CurrentControlSet\Control\Session Manager\Environment';

function JoinPath(const Existing, Dir: string): string;
begin
  if Existing = '' then
    Result := Dir
  else if Pos(';' + Uppercase(Dir) + ';', ';' + Uppercase(Existing) + ';') > 0 then
    Result := Existing
  else
    Result := Existing + ';' + Dir;
end;

function DropPath(const Existing, Dir: string): string;
var
  Rest, Item: string;
  P: Integer;
begin
  Result := '';
  Rest := Existing;
  while Rest <> '' do
  begin
    P := Pos(';', Rest);
    if P = 0 then
    begin
      Item := Rest;
      Rest := '';
    end
    else
    begin
      Item := Copy(Rest, 1, P - 1);
      Rest := Copy(Rest, P + 1, MaxInt);
    end;
    if (Item <> '') and (CompareText(Item, Dir) <> 0) then
    begin
      if Result <> '' then
        Result := Result + ';';
      Result := Result + Item;
    end;
  end;
end;

function SystemPath(Param: string): string;
var
  Orig: string;
begin
  if not RegQueryStringValue(HKLM, EnvKey, 'Path', Orig) then
    Orig := '';
  Result := JoinPath(Orig, ExpandConstant('{app}'));
end;

function VCRedistInstalled: Boolean;
var
  Installed: Cardinal;
begin
  Result := RegQueryDWordValue(HKLM, 'SOFTWARE\Microsoft\VisualStudio\14.0\VC\Runtimes\x64', 'Installed', Installed) and (Installed = 1);
end;

procedure CurUninstallStepChanged(CurUninstallStep: TUninstallStep);
var
  Orig, Updated, Dir: string;
begin
  if CurUninstallStep <> usPostUninstall then
    Exit;
  Dir := ExpandConstant('{app}');
  if not RegQueryStringValue(HKLM, EnvKey, 'Path', Orig) then
    Exit;
  Updated := DropPath(Orig, Dir);
  if Updated <> Orig then
    RegWriteExpandStringValue(HKLM, EnvKey, 'Path', Updated);
end;

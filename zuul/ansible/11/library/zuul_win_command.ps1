#!powershell

# This is based on: https://github.com/ansible-collections/ansible.windows/blob/c3f26bb4fa1b3a08ee2c045d2fdd53e1df171d19/plugins/modules/win_command.ps1
# Copyright: (c) 2017, Ansible Project
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

#AnsibleRequires -CSharpUtil Ansible.Basic
#Requires -Module Ansible.ModuleUtils.Zuul.Win.Command.Process
#Requires -Module Ansible.ModuleUtils.FileUtil

$spec = @{
    options = @{
        _raw_params = @{ type = "str" }
        cmd = @{ type = 'str' }
        argv = @{ type = "list"; elements = "str" }
        chdir = @{ type = "path" }
        creates = @{ type = "path" }
        removes = @{ type = "path" }
        stdin = @{ type = "str" }
        output_encoding_override = @{ type = "str" }
        zuul_log_id = @{ type = "str" }
        zuul_output_max_bytes = @{ type = "int" }
        zuul_no_log = @{ type = "bool" }
    }
    required_one_of = @(
        , @('_raw_params', 'argv', 'cmd')
    )
    mutually_exclusive = @(
        , @('_raw_params', 'argv', 'cmd')
    )
    supports_check_mode = $false
}
$module = [Ansible.Basic.AnsibleModule]::Create($args, $spec)

$chdir = $module.Params.chdir
$creates = $module.Params.creates
$removes = $module.Params.removes
$stdin = $module.Params.stdin
$output_encoding_override = $module.Params.output_encoding_override
$zuul_log_id = $module.Params.zuul_log_id
$zuul_output_max_bytes = $module.Params.zuul_output_max_bytes
$zuul_no_log = $module.Params.zuul_no_log

<#
There are 3 ways a command can be specified with win_command:

    1. Through _raw_params - the value will be used as is

    - win_command: raw params here

    2. Through cmd - the value will be used as is

    - win_command:
        cmd: cmd to run here

    3. Using argv - the values will be escaped using C argument rules

    - win_command:
        argv:
        - executable
        - argument 1
        - argument 2
        - repeat as needed

Each of these options are mutually exclusive and at least 1 needs to be specified.
#>
$filePath = $null
$rawCmdLine = if ($module.Params.cmd) {
    $module.Params.cmd
}
elseif ($module.Params._raw_params) {
    $module.Params._raw_params.Trim()
}
else {
    $argv = $module.Params.argv

    # First resolve just the executable to an absolute path
    $filePath = Resolve-ExecutablePath -FilePath $argv[0] -WorkingDirectory $chdir

    # Then combine the executable + remaining arguments and escape them
    @(
        ConvertTo-EscapedArgument -InputObject $filePath
        $argv | Select-Object -Skip 1 | ConvertTo-EscapedArgument
    ) -join " "
}

$module.Result.cmd = $rawCmdLine
$module.Result.rc = 0

if ($creates -and $(Test-AnsiblePath -Path $creates)) {
    $module.Result.msg = "skipped, since $creates exists"
    $module.Result.skipped = $true
    $module.ExitJson()
}

if ($removes -and -not $(Test-AnsiblePath -Path $removes)) {
    $module.Result.msg = "skipped, since $removes does not exist"
    $module.Result.skipped = $true
    $module.ExitJson()
}

$commandParams = @{
    CommandLine = $rawCmdLine
    ZuulLogId = $zuul_log_id
    ZuulLogPath = "C:/ProgramData/Zuul/Zuul/console-$zuul_log_id.log"
    ZuulOutputMaxBytes = $zuul_output_max_bytes
    ZuulNoLog = $zuul_no_log
}
if ($filePath) {
    $commandParams.FilePath = $filePath
}
if ($chdir) {
    $commandParams.WorkingDirectory = $chdir
}
if ($stdin) {
    $commandParams.InputObject = $stdin
}
if ($output_encoding_override) {
    $commandParams.OutputEncodingOverride = $output_encoding_override
}

$startDatetime = [DateTime]::UtcNow
try {
    $cmdResult = Start-AnsibleWindowsProcess @commandParams
}
catch {
    $module.Result.rc = 2

    # Keep on checking inner exceptions to see if it has the NativeErrorCode to
    # report back.
    $exp = $_.Exception
    while ($exp) {
        if ($exp.PSObject.Properties.Name -contains 'NativeErrorCode') {
            $module.Result.rc = $exp.NativeErrorCode
            break
        }
        $exp = $exp.InnerException
    }

    $module.FailJson("Failed to run: '$rawCmdLine': $($_.Exception.Message)", $_)
}

$module.Result.cmd = $cmdResult.Command
$module.Result.changed = $true
$module.Result.stdout = $cmdResult.Stdout
$module.Result.stderr = $cmdResult.Stderr
$module.Result.rc = $cmdResult.ExitCode

$endDatetime = [DateTime]::UtcNow
$module.Result.start = $startDatetime.ToString("yyyy-MM-dd HH:mm:ss.ffffff")
$module.Result.end = $endDatetime.ToString("yyyy-MM-dd HH:mm:ss.ffffff")
$module.Result.delta = $($endDatetime - $startDatetime).ToString("h\:mm\:ss\.ffffff")

If ($module.Result.rc -ne 0) {
    $module.FailJson("non-zero return code")
}

$module.ExitJson()

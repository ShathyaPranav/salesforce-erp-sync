# Manual setup checklist (things only you can do)

Everything here needs your accounts, a browser, or admin rights, so Claude
Code doesn't do any of it. Work top to bottom. Nothing here goes in chat:
secrets go straight into `.env` (gitignored) or, later, SSM Parameter Store.

## 0. Tools (Windows 11, PowerShell)

Missing today: **Go, AWS SAM CLI, golangci-lint**. Present: git 2.51, Docker
28.3.2 (Desktop 4.44.3), Python 3.12.8, AWS CLI 2.33.9.

```powershell
winget install -e --id GoLang.Go
winget install -e --id Amazon.SAM-CLI
winget install -e --id GolangCI.golangci-lint
```

Open a **new** terminal, then check: `go version` (expect go1.27.1),
`sam --version` (1.166.x), `golangci-lint --version` (2.13.x).

SAM on Windows also wants long paths on. In an **admin** PowerShell:

```powershell
New-ItemProperty -Path "HKLM:\SYSTEM\CurrentControlSet\Control\FileSystem" -Name LongPathsEnabled -Value 1 -PropertyType DWORD -Force
```

**Docker Desktop keeps failing to start** because v4.44.3 leaves two
zero-byte socket files in `%LOCALAPPDATA%\Docker\run` (`dockerInference`,
`userAnalyticsOtlpHttp.sock`) that Windows can't delete. Permanent fix: update
Docker Desktop (Settings > Software updates), or turn off Docker Model Runner
(Settings > AI), which owns `dockerInference`. Workaround until then:

```powershell
wsl -d Ubuntu -- rm -f /mnt/c/Users/luckf/AppData/Local/Docker/run/dockerInference /mnt/c/Users/luckf/AppData/Local/Docker/run/userAnalyticsOtlpHttp.sock
```

## 1. Salesforce Developer Edition org

1. Go to <https://developer.salesforce.com/signup>. Fill in your details and a
   real email you can open. **Username** must look like an email and be
   unique across all of Salesforce, but it doesn't have to be a real mailbox
   (e.g. `shathya.relay@example.dev`). Click **Sign Me Up**.
2. Open the "Verify your account" email, click **Verify Account**, and set a
   password. Write down the username.
3. **Keep the org alive.** Salesforce deletes Developer Edition orgs that
   nobody logs into (45 days for new orgs). API logins may not count. Set a
   monthly reminder to log in through the browser.
4. Setup (gear icon, top right) > Quick Find **My Domain**. Copy **Current My
   Domain URL**, e.g. `https://orgfarm-abc123-dev-ed.develop.my.salesforce.com`.

## 2. External Client App (client credentials flow)

New orgs can't create connected apps any more (Spring '26). An External Client
App (ECA) does the same job for this flow.

1. Setup > Quick Find **External Client App Manager** > **New External Client App**.
   * External Client App Name: `Relay Integration` (API name fills itself)
   * Contact Email: your email (the code to view the secret goes there)
   * Distribution State: **Local**
2. Expand **API (Enable OAuth Settings)**, tick **Enable OAuth**.
   * Callback URL: `https://localhost` (required by the form, never used)
   * OAuth Scopes: move **Manage user data via APIs (api)** to Selected
   * Flow Enablement: tick **Enable Client Credentials Flow** and accept the warning
   * Leave the Security checkboxes at their defaults. Click **Create**.
3. **Set the Run As user.** People most often miss this step. Open the app > **Policies** tab >
   **Edit** > OAuth Policies > *OAuth Flows and External Client App
   Enhancements*: tick **Enable Client Credentials Flow**, and in **Run As
   (Username)** enter your username from step 1.2 (the username, not the
   email). Save. Skip this and every token request fails with
   `invalid_grant: no client credentials user enabled`.
4. **Settings** tab > **OAuth Settings** > **Consumer Key and Secret**. Enter the
   emailed code. Copy the two values straight into `.env` (next section).
5. Wait about 10 minutes. New app credentials take a while to propagate, and
   until then you get `invalid_client_id`.

(Better practice, optional: a dedicated integration user with the
"Salesforce Integration" licence and read-only access to Account and
Opportunity as the Run As user. It's a good least-privilege point to make
in an interview, but your admin user works fine for this project.)

## 3. `.env`

```powershell
Copy-Item .env.example .env
notepad .env
```

| Key | Value |
| --- | --- |
| `SF_LOGIN_URL` | your My Domain URL from 1.4, no trailing slash |
| `SF_CLIENT_ID` | Consumer Key from 2.4 |
| `SF_CLIENT_SECRET` | Consumer Secret from 2.4 |
| `SF_API_VERSION` | `v66.0` (leave as is) |

Test it (never prints the secret or token):

```powershell
C:\Users\luckf\.venvs\relay\Scripts\python scripts\sf_query.py
```

You should see `Authenticated. instance_url=...` then your Closed Won deals.
That completes Phase 1.1's last "done when".

## 4. Sample data

Your new org already has Salesforce's demo Accounts and Opportunities (Edge
Communications, United Oil & Gas and so on). Some of them are Closed Won, so
they'll sync too. That's fine and gives you more data. Add these as well, which
mirror `fake_salesforce/seed.json`:

**Accounts** (App Launcher > Accounts > New): `Acme Robotics`, `Globex Foods`,
`Initech Software`, `Umbrella Health`.

**Opportunities** (App Launcher > Opportunities > New; Name, Close Date and
Stage are required):

| Name | Account | Amount | Stage | Tests |
| --- | --- | --- | --- | --- |
| Acme - 50 robot arms | Acme Robotics | 125000 | Closed Won | happy path |
| Globex - cold storage retrofit | Globex Foods | 48000 | Closed Won | happy path |
| Initech - site licence | Initech Software | *(blank)* | Closed Won | **no Amount**: permanent error, goes to the DLQ with a reason |
| Umbrella - pilot | Umbrella Health | 12000 | Closed Won | **edited twice** (below) |
| Orphan deal - no account | *(blank)* | 5000 | Closed Won | no Account: permanent error |
| Globex - renewal | Globex Foods | 30000 | Negotiation/Review | must never sync |
| Initech - hardware | Initech Software | 22000 | Closed Lost | must never sync |

* **Edited twice:** edit *Umbrella - pilot* to Amount 15000 and save, wait a
  few seconds, then edit it to 18000 and save. Each save gives a new
  `SystemModstamp`, which is the version the ERP compares. It's more
  interesting to do the two edits again **after** the first AWS deploy (Phase 2.1), so you
  can watch the order's `version` and `revision` change in DynamoDB.
* If the form won't let you leave Account blank, Setup > Object Manager >
  Opportunity > Page Layouts > Opportunity Layout > make Account Name not
  required. Or skip that row: the fake Salesforce covers it.
* Setting a deal to Closed Won may change its Close Date to today. That's normal.
* You can't create the "two deals with the same SystemModstamp" case by hand.
  The fake Salesforce covers that one.

Then record the real response for the contract test:

```powershell
C:\Users\luckf\.venvs\relay\Scripts\python scripts\sf_query.py --record tests\contract\fixtures\real_query_response.json
```

## 5. AWS account and $1 budget

**Know your plan.** Accounts created after 15 July 2025 choose *Free* or
*Paid*. A **Free-plan account closes automatically after 6 months** (or when
its credits run out). Put that date in your calendar and upgrade before then
if the demo must stay up. Don't enable AWS Organizations or IAM Identity
Center: either one upgrades the account to Paid.

1. Sign in as root. Top-right menu > **Security credentials** > assign an **MFA** device.
2. Billing console > **Account** > *IAM user and role access to Billing
   information* > Edit > tick **Activate IAM access** > Update.
3. IAM > Users > **Create user** `relay-admin`: tick *Provide user access to
   the AWS Management Console*, *I want to create an IAM user*, custom
   password. Attach policy **AdministratorAccess**. **Don't create access
   keys.** Sign in as `relay-admin` and give it MFA too.
4. **Budget** (as relay-admin): Billing and Cost Management > **Budgets** >
   **Create budget** > **Customize (advanced)** > **Cost budget** > Next.
   * Name `relay-1-dollar`; Period **Monthly**; **Recurring**; **Fixed**; amount **1.00**
   * Budget scope > **Advanced options**: **exclude Credits** (and Refunds), or pick
     *Unblended costs*, not "net". Otherwise your $100+ of credits hide
     every charge and the alert never fires.
   * Alert: threshold **100 %** of budgeted amount, trigger **Actual**, your
     email. Optional: a second one at 100 % **Forecasted**. Forecasts need a few
     weeks of history, so they won't fire at first.
   * Skip actions > **Create budget**. Budget data refreshes a few times a day,
     so it's an early warning, not a hard limit. The real protection is the
     no-NAT, no-VPC, no-hourly-services rule.
   * If the console home page offers an "Explore AWS" credit for setting up a
     budget, create it from there to collect the credit, then edit it as above.

## 6. AWS CLI on this machine (no access keys)

CLAUDE.md says no long-lived AWS keys, so use `aws login` (browser sign-in,
short-lived credentials that refresh themselves; your CLI 2.33.9 has it) into a
named profile. Don't create a `[default]` profile, so nothing run without
`--profile` can ever reach your real account.

```powershell
aws login --profile relay          # browser opens; sign in as relay-admin; region: us-east-1
aws sts get-caller-identity --profile relay
```

You should see your 12-digit account ID and `user/relay-admin`. The session
lasts up to 12 hours; run `aws login --profile relay` again when it expires.

## Later (you'll be asked at the right phase)

* **Phase 2.1:** put the Salesforce secret into SSM as a SecureString. CloudFormation
  can't create SecureStrings, so this is a manual step: Systems Manager >
  Parameter Store > Create parameter, name `/relay/salesforce/client_secret`,
  type SecureString, default key. Using the console keeps it out of your shell
  history. Approve the first `sam deploy`.
* **Phase 2.2:** create the GitHub repo, approve the one-time OIDC bootstrap
  (the role CI assumes has to exist before CI can use it), approve the push.
* **Phase 2.3:** click **Confirm subscription** in the SNS alarm email within 48 hours (check spam).

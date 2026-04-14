# MODULE 8: SECURITY, GOVERNANCE & COMPLIANCE


## Q31. How would you design IAM roles so a Glue job reading/writing customer PII in S3 has 
only minimum permissions? 
I apply strict least-privilege, scoped to just the data paths, keys, and services this single job
needs. I separate duties between the job runtime, catalog/governance, and administration.
1.  Create a dedicated Glue job role
• Trust policy allows only the Glue service to assume it.
• No wildcard admin policies attached; start from empty and add only what’s required.
2.  Scope S3 access to exact prefixes (or an access point)
• Allow GetObject/List on the specific input prefix like s3://company-lake/pii/sourceA/
and PutObject on the specific output prefix like s3://company-lake/pii/curatedA/.
• In the bucket policy, additionally restrict by aws:PrincipalArn to this role and by
s3:prefix so it can’t read other folders.
• Prefer an S3 Access Point (or VPC Access Point) limited to those prefixes and to the
VPC used by the job.
3.  Enforce TLS and private access
• Bucket policy denies if aws:SecureTransport is false.
• If the job runs in a VPC, use an S3 Gateway/Interface VPC endpoint and bucket policy
with aws:SourceVpce so traffic never leaves AWS backbone.
4.  Lock down KMS usage (SSE-KMS everywhere)
• Give the role kms:Decrypt/kms:Encrypt only on the specific CMK used for these
datasets.
• In the CMK key policy, allow only this role (and key admins) and add a kms:ViaService
condition for s3.<region>.amazonaws.com and logs.<region>.amazonaws.com.
• Deny wildcards like kms:* on all keys.
5.  Minimal Glue permissions
• glue:GetTable/GetPartition on only the required databases/tables. No Create/Update
unless the job truly needs to write metadata.
• If Lake Formation is enabled, grant data permissions there (column/table/row) and
keep IAM policies metadata-only.
6.  JDBC/warehouse access kept separate
• If the job touches Redshift/JDBC, use Secrets Manager for credentials. Grant the role
only secretsmanager:GetSecretValue for that one secret and nothing else in Secrets
Manager.
• Redshift IAM role for COPY/UNLOAD is separate and scoped to the same S3 prefixes
and KMS key.
7.  Guardrails and denies
• Service Control Policies or bucket “explicit deny” to prevent any principal except
approved roles from touching pii= true resources (use resource tags + aws:ResourceTag
conditions).
• CloudWatch Logs permissions limited to one log group.
• No PassRole permission to avoid privilege escalation.
8.  Observability and review
• Enable CloudTrail and S3 server access logs.
• Use IAM Access Analyzer/last accessed data to prune unused permissions after a
week of running.
This gives a single Glue job role with the smallest possible set of actions on just the right S3
prefixes, the one KMS key, the necessary Catalog entries, and nothing else.
 



## Q32. How would you ensure Glue pipelines encrypt data at rest and in transit (S3, Glue, 
Redshift) to meet compliance? 
I enable encryption by default at every layer (S3, Glue, Redshift, logs, checkpoints) and force
TLS on all network paths. I also prove it with policies that reject non-compliant writes.
1.  S3 at rest
• Turn on default bucket encryption with SSE-KMS using a customer-managed CMK.
• Bucket policy requires x-amz-server-side-encryption = aws:kms and the specific KMS
key ARN, and denies requests without it.
• Enforce aws:SecureTransport true and optionally aws:SourceVpce to keep traffic on
VPC endpoints.
• Encrypt Glue checkpoints/temp paths with the same CMK.
2.  Glue job/runtime
• Create a Glue “security configuration” that enables: S3 encryption (SSE-KMS CMK),
job bookmark encryption, CloudWatch Logs encryption, and Spark UI logs encryption.
Attach this config to every job.
• Run jobs in a private VPC with no public subnets; route S3 via VPC endpoints so traffic
stays private.
• For JDBC sources/targets, require SSL (set connection property useSSL/ssl=true and
validate certs).
3.  In transit everywhere
• S3: HTTPS only via aws:SecureTransport in bucket policy.
• Redshift: set require SSL in the parameter group and use jdbc:redshift:ssl=true; block
non-SSL at the security group/NACL.
• Kafka/Kinesis: use TLS endpoints; for Kafka, configure ssl.* properties.
• Internal services (Athena/Glue Data Catalog): traffic is TLS by default; avoid custom
endpoints that bypass TLS.
4.  Redshift at rest
• Enable cluster encryption with KMS (CMK).
• Encrypt automated and manual snapshots and cross-region snapshot copies with
KMS.
• COPY/UNLOAD use IAM roles scoped to the encrypted S3 prefixes and CMK; require
UNLOAD to specify ENCRYPTED if plain CSV is used (Parquet + SSE-KMS preferred).
5.  Key management and access
• CMK key policies restrict usage to specific roles; enable automatic key rotation.
• Use kms:ViaService conditions to limit CMK use to S3, Logs, and Redshift where
applicable.
• No broad kms:* permissions; use grants for specific services if needed.
6.  Governance and proof
• Lake Formation governs data access; column masks and row filters apply after
encryption so only authorized users see decrypted data through Athena/EMR.
• CloudTrail logs are encrypted and retained; Config rules or Security Hub controls alert
on buckets without SSE-KMS or policies missing SecureTransport.
• Periodically run a “deny list” control: SCPs that deny s3:PutObject without KMS on
accounts handling PII.
7.  Edge cases
• Temporary files: ensure Glue temp directories and Spark shuffle spill paths are in
encrypted buckets.
• Third-party sinks: use TLS endpoints and store creds in Secrets Manager, not in code;
restrict the role to read only that secret.
 
With these settings, every byte written is SSE-KMS encrypted by default, every connection is
over TLS, and guardrail policies enforce and audit the posture so the pipeline stays compliant.



## Q33. How would you use Lake Formation to let analysts query Athena tables but hide 
sensitive PII columns? 
I put the S3 data under Lake Formation governance and grant analysts only the columns they’re
allowed to see. Athena enforces these permissions automatically.
1.  register the S3 locations in Lake Formation and turn on LF permissions for the Glue Data
Catalog
2.  create or crawl the tables in the Glue Catalog as governed tables
3.  tag columns with lf-tags (for example, pii=yes/no, domain=sales) or select columns
explicitly
4.  grant analysts select only on approved columns (either via lf-tag expressions like pii=no,
or by listing specific columns)
5.  optionally add row filters and column masks if needed (for example, hash(email) or
show last 4 of phone)
6.  grant engineers broader access using separate roles; analysts’ role gets only columnlevel
select
7.  ensure Athena workgroups use Lake Formation authorization and that S3 bucket
policies allow access via Lake Formation
8.  audit with Lake Formation access logs to confirm analysts never read
masked/forbidden columns
Result: analysts can run the same SQL in Athena, but sensitive columns are hidden or masked
by Lake Formation policies.
 



## Q34. If a Glue job role has S3 permissions but still can’t access a bucket, how would you
troubleshoot IAM/trust issues? 
I trace the request path end to end and look for explicit denies, missing KMS rights, or the wrong
role being assumed.
1.  confirm the runtime principal
check the job details and CloudTrail to ensure the job is actually running as the
intended iam role. print sts get-caller-identity from the job if needed. make sure only
one job role is configured and that the trust policy allows glue.amazonaws.com to
assume it.
2.  look for explicit denies in bucket or org
inspect the bucket policy for denies on aws:principalarn, aws:sourcevpce, aws:userid,
aws:securetransport, or ip conditions. check organization scps and permission
boundaries that may override allows.
3.  verify kms permissions and key policy
if the bucket enforces sse-kms with a specific cmk, the role must have
kms:decrypt/encrypt and the key policy must allow the role (or a grant). also check
kms:viaservice conditions match s3 in the right region.
4.  check the exact s3 actions and resources
ensure the role has s3:listbucket on the bucket arn (with appropriate s3:prefix
conditions) and s3:getobject/s3:putobject on object arns. many failures are just missing
listbucket on the bucket while getobject is granted on objects.
5.  validate the path and access points
confirm the code is hitting the correct bucket/prefix and, if using an s3 access point,
that the access point policy allows this role and account. avoid typos and trailing
slashes that break prefix conditions.
6.  network and endpoint constraints
if the bucket policy requires a specific vpc endpoint (aws:sourcevpce), make sure the
glue job runs in that vpc and subnets, and dns to s3 is via the endpoint. enforce https by
setting aws:securetransport true.
7.  lake formation vs direct s3
if reading through a governed table (athena/spark with lf), lack of lf permissions can look
like s3 access denied. grant the table/column permissions in lake formation or bypass lf
by reading s3 paths directly.
8.  object ownership and acl quirks
if objects are written by another account, enable bucket owner enforced object
ownership or ensure the role has access via acls. mismatched ownership commonly
blocks reads even when bucket policy looks fine.
9.  reproduce and pinpoint
use the iam policy simulator with the role, action, and resource. check cloudtrail events
for the exact deny reason. try aws s3 ls s3://bucket/prefix from within the job to capture
the error verbatim.
fixes usually involve correcting the assumed role or trust policy, adding listbucket, granting kms
on the right key with a matching key policy, or removing an explicit deny in the bucket or scp.
All rig hts reserved. Personal use only. Redistribution or resale is prohibited  
 



## Q35. How would you design Glue jobs to mask or delete personal data (PII) on request to 
meet GDPR requirements? 
I separate two capabilities: ongoing masking for analytics, and subject-driven erasure (Right to
be Forgotten). Both run as controlled, auditable Glue jobs with strict least-privilege roles and
KMS encryption.
Ongoing masking for analytics
• Identify PII columns in the Catalog using tags (for example, pii=email, phone, address).
• In ingestion/curation Glue jobs, apply deterministic masking before data lands in the analytics
zone:
– Tokenize IDs with a salted hash (for example, SHA-256(customer_id + org_salt)) so joins still
work.
– Partially mask free-text (keep last 4 digits of phone, mask rest).
– Use format-preserving encryption for values that must look valid (for example, card PAN in PCI
contexts).
– Drop sensitive columns entirely if not needed.
• Keep a separate, access-restricted “gold-pii” table only if a legitimate purpose exists;
otherwise, store only masked columns.
• Enforce access with Lake Formation column masks so analysts never see raw PII.
Subject-driven erasure (DSAR)
• Maintain a subject ID map (for example, by customer_id or a stable subject_key) that lists all
tables/partitions where the subject appears. Keep this map updated during ingestion.
• Build a parameterized “erasure” Glue job: input is the subject_key list from your DSAR
system. The job:
1.  Locates all occurrences across S3 lakehouse tables (Delta/Hudi/Iceberg) using
partition filters to minimize scans.
2.  For soft-delete, writes tombstones/upserts to set PII fields to null or masked
equivalents immediately, so downstream queries stop seeing PII.
3.  For hard-delete, issues deletes in the table format and triggers compaction/vacuum to
physically remove data from files (required because S3 is immutable at file level).
• For non-ACID raw zones (plain Parquet), rewrite affected partitions: read → filter out
subject rows → overwrite partition atomically to a new prefix → swap pointer.
• For Redshift or JDBC stores, run a transactional DELETE or UPDATE to null out PII,
followed by VACUUM/ANALYZE if needed.
Operational controls and audit
• All jobs run with security configurations: SSE-KMS for S3, encrypted bookmarks/logs, TLS to
sources/targets.
• Keep minimal audit logs showing request ID, tables touched, row counts removed or masked
(never log raw PII).
• Implement retries with idempotency: if the job re-runs for the same subject, it produces the
same state.
• Enforce SLA (for example, 30 days) by scheduling daily erasure windows and alerting on any
missed subjects.
• Validate with data quality checks that no PII remains in analytics tables after the job.
Result: analytics never sees raw PII, and DSAR erasure reliably nulls or removes personal data
across lake/warehouse with audit proof.
 



## Q48. How would you use Glue + Lake Formation to give analysts access only to curated data, 
while engineers can access both raw and curated? 
I put the Catalog under Lake Formation governance, separate users into analyst and engineer
groups, and grant permissions at database/table/column level so analysts never touch raw.
1.  Separate databases by zone
Keep raw and curated in different Glue databases (for example, retail_raw,
retail_curated). Register their S3 locations in Lake Formation.
2.  Define groups and roles
Create two IAM roles (or SSO groups): analysts and engineers. Engineers get broader
rights; analysts are read-only on curated.
3.  Apply Lake Formation permissions
Grant analysts SELECT on the curated databases/tables (and only specific columns if
needed using column-level grants or LF-Tags like pii=no). Do not grant them anything on
raw. Grant engineers SELECT on both raw and curated, plus optional ALTER/CREATE if
they manage pipelines.
4.  Enforce S3 via LF
Use bucket policies that allow access “via Lake Formation” and deny direct S3 reads to
raw prefixes for analyst roles. This ensures all access checks go through LF policies.
5.  Optional row filters and masks
For curated, add row-level filters (for example, region-based) and column masks for
semi-sensitive fields. Engineers can see full data; analysts see filtered/masked views.
6.  Register curated tables once, avoid ad-hoc schemas
Glue jobs that produce curated data register/update tables programmatically. Analysts
never create tables; they just query. For partitioned tables, enable partition projection
so analysts can query new partitions instantly without crawler runs.
7.  Auditing and guardrails
Enable Lake Formation and CloudTrail audit logs. Create a periodic report of who
accessed what. Add an SCP that blocks CreateTable in Glue for analyst roles to prevent
shadow tables.
8.  Onboarding checklist
When a new curated dataset is published: set owner/contact, tag columns with
pii=yes/no, grant analysts via LF-Tag expression (pii=no), test access with an analyst
role, and publish sample Athena queries.
Result: analysts can query only curated, non-sensitive columns through Athena; engineers can
work across raw and curated. Policies are enforced centrally by Lake Formation, and direct S3
access to raw is blocked for analysts.
 

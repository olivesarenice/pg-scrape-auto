# Deployment

Use https://www.platform.ploomber.io/ for quick deployment
- needs requirements.txt --> means I will have to write a script that will auto call the Ploomber CLI to reupload the zip each time we want to redeploy.
- AWS Fargate expensive 9$ USD for the smallest compute running 24/7 per month.
- Ploomber can use custom DNS, need to setup records: https://ploomber.io/blog/streamlit-custom-domain/

# Remaining work

1. Add last page for historical trends
2. Deploy app and point DNS to oliverqsite domain
3. Write deployment scripts
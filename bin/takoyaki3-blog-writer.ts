#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { Takoyaki3BlogWriterStack } from '../lib/takoyaki3-blog-writer-stack';

const app = new cdk.App();

// The Gemini API key is supplied via the GEMINI_API_KEY environment variable
// (e.g. from GitHub Actions secrets) rather than Secrets Manager.
const geminiApiKey = process.env.GEMINI_API_KEY;
if (!geminiApiKey) {
  // eslint-disable-next-line no-console
  console.warn(
    'WARNING: GEMINI_API_KEY is not set. The generation worker will fail to call Gemini until it is provided.'
  );
}

new Takoyaki3BlogWriterStack(app, 'Takoyaki3BlogWriterStack', {
  // The AWS region is fixed to Tokyo (ap-northeast-1) inside the stack
  // definition, so it does not need to be specified here or in the workflow.

  geminiApiKey,

  /* For more information, see https://docs.aws.amazon.com/cdk/latest/guide/environments.html */
});
# Takoyaki3 Blog Writer

写真アップロードから下書き生成までを AWS サーバレス構成で行うブログ生成基盤です。Gemini を使って下書きの Markdown を生成します。

## 概要
- 署名付き URL で写真を S3 にアップロード
- メタデータ処理と任意の逆ジオコーディングをキューに投入
- Gemini で Markdown 下書きを生成し DynamoDB に保存
- 最小の操作 UI を S3 + CloudFront で配信

## 構成
- API Gateway + Lambda（アップロード、完了通知、生成、再生成、取得）
- S3（uploads, content, site）
- DynamoDB（uploads, photo metadata, articles, generation runs）
- SQS（exif queue, generation queue）
- AWS Location Service（逆ジオコーディング用 Place Index）
- Secrets Manager（Gemini API Key）
- CloudFront（静的 UI）

## API
Base URL: `https://.../`

### POST /v1/uploads
署名付き URL を作成します。

Request:
```json
{
  "filename": "photo.jpg",
  "content_type": "image/jpeg",
  "user_id": "optional"
}
```

Response に `upload_id`, `upload_url`, `object_key` が含まれます。

### POST /v1/uploads/{uploadId}/complete
アップロード完了を通知し、メタデータ処理をキューに送ります。

Request:
```json
{
  "object_key": "uploads/<upload_id>/photo.jpg"
}
```

### POST /v1/articles/generate
新規の下書き生成をキューに入れます。

Request:
```json
{
  "upload_ids": ["uuid-1", "uuid-2"],
  "user_id": "optional",
  "tone": "polite|casual|formal",
  "length": "short|medium|long",
  "language": "ja|en",
  "privacy_level": "area|city|exact",
  "instruction": "optional"
}
```

Response に `article_id` と `run_id` が含まれます。

### POST /v1/articles/{articleId}/regenerate
既存記事に対して新しい下書きをキューに入れます。

Request:
```json
{
  "tone": "polite|casual|formal",
  "length": "short|medium|long",
  "language": "ja|en",
  "privacy_level": "area|city|exact",
  "instruction": "optional"
}
```

### GET /v1/articles/{articleId}
記事データを取得します。`body_markdown` と `body_json` を含みます。

## デプロイ
前提: Node.js, AWS CDK v2, AWS 認証情報。

```bash
npm install
npm run build
npx cdk bootstrap
npx cdk deploy
```

出力に `ApiEndpoint` と `WebsiteUrl` が含まれます。

## Gemini API キー
スタックで `takoyaki3-blog-writer/gemini-api-key` を作成します。
Secret の中身は生のキー文字列か JSON のどちらでも可です。

```json
{
  "apiKey": "YOUR_KEY"
}
```

キー名は `apiKey`, `key`, `GEMINI_API_KEY` を受け付けます。

## Web UI
静的 UI は `web/` にあります。`config.json` を参照します。

```json
{
  "apiBaseUrl": "https://.../prod"
}
```

CDK デプロイ時にホスト先用の `config.json` を自動生成します。

## Gemini に渡すコンテキスト
生成ワーカーは以下の情報を Gemini に渡します。

- 設定値: 言語、文体、長さ、公開範囲、ユーザー指示、最小文字数
- ルール: 具体的なカメラ・時間・場所の詳細は推測しない（不明なら `unknown` / `unspecified`）
- 画像: S3 から取得した写真データを inline で添付
- 写真メタデータ: アップロード ID、S3 参照、撮影日時、アップロード日時、カメラメーカー/モデル、逆ジオコーディング結果、Content-Type、サイズ（KB）
- 出力形式: JSON（`title`, `date`, `location`, `tags`, `body_markdown`, `capture_info`）

## 注意点
- 本リポジトリ内では EXIF 抽出は未実装です。メタデータ worker は S3 の HEAD 情報と、SQS メッセージに含まれる GPS 情報を利用します。
- 下書きは `ArticlesTable` の `body_markdown` と `body_json` に保存されます。

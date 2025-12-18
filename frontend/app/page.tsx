'use client'

import { useEffect, useState, useRef, useCallback } from 'react'

interface Summary {
  file_id: string
  filename: string
  status: string
  text: string
  summary: string
  updated_at: string
}

interface SummariesResponse {
  summaries: Summary[]
  count: number
}

export default function Home() {
  const [summaries, setSummaries] = useState<Summary[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [uploading, setUploading] = useState(false)
  const [uploadError, setUploadError] = useState<string | null>(null)
  const [uploadSuccess, setUploadSuccess] = useState(false)
  const [isDragging, setIsDragging] = useState(false)
  const [extractionMode, setExtractionMode] = useState('plain_text')
  const fileInputRef = useRef<HTMLInputElement>(null)

  const fetchSummaries = useCallback(async (silent = false) => {
    try {
      const apiUrl = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'
      const response = await fetch(`${apiUrl}/summaries`)
      
      if (!response.ok) {
        throw new Error('Failed to fetch summaries')
      }
      
      const data: SummariesResponse = await response.json()
      setSummaries(data.summaries || [])
    } catch (err) {
      if (!silent) {
        setError(err instanceof Error ? err.message : 'An error occurred')
      }
    } finally {
      if (!silent) {
        setLoading(false)
      }
    }
  }, [])

  useEffect(() => {
    fetchSummaries()
  }, [fetchSummaries])

  // Poll for status updates when there are summaries in progress
  useEffect(() => {
    const hasInProgressSummaries = summaries.some(
      (s) => s.status.toLowerCase() === 'uploaded' || s.status.toLowerCase() === 'text_ready'
    )

    if (!hasInProgressSummaries) {
      return
    }

    const pollInterval = setInterval(() => {
      fetchSummaries(true) // Silent fetch to avoid showing loading state
    }, 3000) // Poll every 3 seconds

    return () => clearInterval(pollInterval)
  }, [summaries, fetchSummaries])

  const handleFileUpload = async (file: File) => {
    // Validate file type
    if (file.type !== 'application/pdf' && !file.name.toLowerCase().endsWith('.pdf')) {
      setUploadError('Only PDF files are allowed')
      return
    }

    // Validate file size (5MB max)
    const maxSize = 5 * 1024 * 1024 // 5MB
    if (file.size > maxSize) {
      setUploadError('File size must be less than 5MB')
      return
    }

    setUploading(true)
    setUploadError(null)
    setUploadSuccess(false)

    try {
      const apiUrl = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'
      const formData = new FormData()
      formData.append('file', file)

      const modeParam = encodeURIComponent(extractionMode)
      const response = await fetch(`${apiUrl}/summarize?mode=${modeParam}`, {
        method: 'POST',
        body: formData,
      })

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({ detail: 'Upload failed' }))
        throw new Error(errorData.detail || 'Failed to upload PDF')
      }

      const data = await response.json()
      setUploadSuccess(true)
      
      // Refresh summaries list
      await fetchSummaries()
      
      // Reset success message after 3 seconds
      setTimeout(() => {
        setUploadSuccess(false)
      }, 3000)
    } catch (err) {
      setUploadError(err instanceof Error ? err.message : 'An error occurred during upload')
    } finally {
      setUploading(false)
    }
  }

  const handleFileSelect = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (file) {
      handleFileUpload(file)
    }
    // Reset input so same file can be selected again
    if (fileInputRef.current) {
      fileInputRef.current.value = ''
    }
  }

  const handleDragOver = (e: React.DragEvent) => {
    e.preventDefault()
    e.stopPropagation()
    setIsDragging(true)
  }

  const handleDragLeave = (e: React.DragEvent) => {
    e.preventDefault()
    e.stopPropagation()
    setIsDragging(false)
  }

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault()
    e.stopPropagation()
    setIsDragging(false)

    const file = e.dataTransfer.files?.[0]
    if (file) {
      handleFileUpload(file)
    }
  }

  const handleButtonClick = () => {
    fileInputRef.current?.click()
  }

  const getStatusColor = (status: string) => {
    switch (status.toLowerCase()) {
      case 'uploaded':
        return 'bg-blue-100 text-blue-800'
      case 'text_ready':
        return 'bg-yellow-100 text-yellow-800'
      case 'summary_ready':
        return 'bg-green-100 text-green-800'
      case 'error':
        return 'bg-red-100 text-red-800'
      default:
        return 'bg-gray-100 text-gray-800'
    }
  }

  return (
    <main className="min-h-screen bg-gradient-to-br from-gray-50 to-gray-100 py-8 px-4">
      <div className="max-w-7xl mx-auto">
        <div className="mb-8">
          <h1 className="text-4xl font-bold text-gray-900 mb-2">PDF Summary Application</h1>
          <p className="text-gray-600">View all PDF summaries</p>
        </div>

        {/* Upload Form */}
        <div className="mb-8">
          {/* Text Extraction Mode Selection */}
          <div className="mb-4 bg-white rounded-lg shadow-sm p-4">
            <label className="block text-sm font-medium text-gray-700 mb-3">
              Text extraction mode
            </label>
            <div className="flex gap-6">
              <label className="flex items-center cursor-pointer">
                <input
                  type="radio"
                  name="extractionMode"
                  value="plain_text"
                  checked={extractionMode === 'plain_text'}
                  onChange={(e) => setExtractionMode(e.target.value)}
                  className="w-4 h-4 text-blue-600 focus:ring-blue-500"
                />
                <span className="ml-2 text-gray-700">Plain text</span>
              </label>
              <label className="flex items-center cursor-pointer">
                <input
                  type="radio"
                  name="extractionMode"
                  value="markdown"
                  checked={extractionMode === 'markdown'}
                  onChange={(e) => setExtractionMode(e.target.value)}
                  className="w-4 h-4 text-blue-600 focus:ring-blue-500"
                />
                <span className="ml-2 text-gray-700">Markdown</span>
              </label>
            </div>
          </div>

          <div
            onDragOver={handleDragOver}
            onDragLeave={handleDragLeave}
            onDrop={handleDrop}
            className={`border-2 border-dashed rounded-lg p-8 text-center transition-colors ${
              isDragging
                ? 'border-blue-500 bg-blue-50'
                : 'border-gray-300 bg-white hover:border-gray-400'
            }`}
          >
            <input
              ref={fileInputRef}
              type="file"
              accept=".pdf,application/pdf"
              onChange={handleFileSelect}
              className="hidden"
              disabled={uploading}
            />
            
            <div className="flex flex-col items-center justify-center space-y-4">
              <svg
                className="w-16 h-16 text-gray-400"
                fill="none"
                stroke="currentColor"
                viewBox="0 0 24 24"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12"
                />
              </svg>
              
              <div>
                <p className="text-lg font-medium text-gray-700 mb-1">
                  {isDragging ? 'Drop your PDF here' : 'Drag and drop your PDF file here'}
                </p>
                <p className="text-sm text-gray-500 mb-4">or</p>
                <button
                  onClick={handleButtonClick}
                  disabled={uploading}
                  className="px-6 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:bg-gray-400 disabled:cursor-not-allowed transition-colors font-medium"
                >
                  {uploading ? 'Uploading...' : 'Browse Files'}
                </button>
              </div>
              
              <p className="text-xs text-gray-400 mt-2">
                PDF files only • Maximum size: 5MB
              </p>
            </div>
          </div>

          {uploadError && (
            <div className="mt-4 bg-red-50 border border-red-200 text-red-800 px-4 py-3 rounded-lg">
              <p className="font-semibold">Upload Error:</p>
              <p>{uploadError}</p>
            </div>
          )}

          {uploadSuccess && (
            <div className="mt-4 bg-green-50 border border-green-200 text-green-800 px-4 py-3 rounded-lg">
              <p className="font-semibold">Success!</p>
              <p>PDF uploaded successfully and is being processed.</p>
            </div>
          )}
        </div>

        {loading && (
          <div className="flex justify-center items-center py-12">
            <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600"></div>
          </div>
        )}

        {error && (
          <div className="bg-red-50 border border-red-200 text-red-800 px-4 py-3 rounded-lg mb-6">
            <p className="font-semibold">Error:</p>
            <p>{error}</p>
          </div>
        )}

        {!loading && !error && summaries.length === 0 && (
          <div className="bg-white rounded-lg shadow-md p-8 text-center">
            <p className="text-gray-500 text-lg">No summaries found</p>
            <p className="text-gray-400 mt-2">Upload a PDF to get started</p>
          </div>
        )}

        {!loading && !error && summaries.length > 0 && (
          <div className="grid gap-6 mt-8">
            {summaries.map((summary) => (
              <div
                key={summary.file_id}
                className="bg-white rounded-lg shadow-md hover:shadow-lg transition-shadow duration-200 p-6"
              >
                <div className="flex flex-col md:flex-row md:items-start md:justify-between mb-4">
                  <div className="flex-1">
                    <h2 className="text-xl font-semibold text-gray-900 mb-2">
                      {summary.filename || 'Untitled PDF'}
                    </h2>
                    <div className="flex flex-wrap gap-2 items-center mb-3">
                      <span className="text-sm text-gray-500">
                        <span className="font-medium">File ID:</span> {summary.file_id}
                      </span>
                    </div>
                  </div>
                  <span
                    className={`inline-flex items-center px-3 py-1 rounded-full text-sm font-medium ${getStatusColor(
                      summary.status
                    )}`}
                  >
                    {summary.status}
                  </span>
                </div>

                {summary.summary && (
                  <div className="mt-4 pt-4 border-t border-gray-200">
                    <h3 className="text-sm font-semibold text-gray-700 mb-2">Summary:</h3>
                    <p className="text-gray-600 leading-relaxed">{summary.summary}</p>
                  </div>
                )}

                {summary.updated_at && (
                  <div className="mt-4 text-xs text-gray-400">
                    Last updated: {new Date(summary.updated_at).toLocaleString()}
                  </div>
                )}
              </div>
            ))}
          </div>
        )}

        {!loading && !error && summaries.length > 0 && (
          <div className="mt-6 text-center text-gray-500 text-sm">
            Showing {summaries.length} {summaries.length === 1 ? 'summary' : 'summaries'}
          </div>
        )}
      </div>
    </main>
  )
}


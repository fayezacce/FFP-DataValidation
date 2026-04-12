/**
 * Authentication Helpers for FFP Data Validator
 */

const STORAGE_KEY = "ffp_auth_token";
const USER_KEY = "ffp_user";

export const getBackendUrl = () => {
  // In development, the proxy is handled by Nginx at localhost:3000/api
  // In production, it's the same origin
  return "/api";
};

export const getToken = () => {
  if (typeof window !== "undefined") {
    return localStorage.getItem(STORAGE_KEY);
  }
  return null;
};

export const setToken = (token: string) => {
  if (typeof window !== "undefined") {
    localStorage.setItem(STORAGE_KEY, token);
  }
};

export const clearToken = () => {
  if (typeof window !== "undefined") {
    localStorage.removeItem(STORAGE_KEY);
    localStorage.removeItem(USER_KEY);
  }
};

export const getUser = () => {
  if (typeof window !== "undefined") {
    const user = localStorage.getItem(USER_KEY);
    return user ? JSON.parse(user) : null;
  }
  return null;
};

export const setUser = (user: any) => {
  if (typeof window !== "undefined") {
    localStorage.setItem(USER_KEY, JSON.stringify(user));
  }
};

export const isAdmin = () => {
  const user = getUser();
  return user?.role === "admin";
};

export const isAuthenticated = () => {
  const token = getToken();
  if (!token) return false;
  
  try {
    // Basic JWT check for expiry
    const payload = JSON.parse(atob(token.split(".")[1]));
    const now = Math.floor(Date.now() / 1000);
    return payload.exp > now;
  } catch (e) {
    return false;
  }
};

export const fetchWithAuth = async (url: string, options: RequestInit = {}) => {
  const token = getToken();
  const headers = {
    ...options.headers,
    ...(token ? { "Authorization": `Bearer ${token}` } : {}),
  } as any;

  const response = await fetch(url, { ...options, headers });
  
  if (response.status === 401) {
    clearToken();
    if (typeof window !== "undefined" && !window.location.pathname.startsWith("/login")) {
      window.location.href = "/login";
    }
  }
  
  // Security lockout: show the specific message from the backend
  if (response.status === 503) {
    try {
      const body = await response.clone().json();
      if (body.detail && body.detail.includes("lockout")) {
        alert("⚠️ Security Lockout: " + body.detail);
      }
    } catch { /* ignore parse errors */ }
  }
  
  return response;
};

/**
 * Authenticated Download Helper
 * Uses fetch with auth header and creates a Blob for download
 */
export const downloadFileWithAuth = async (
  url: string, 
  filename?: string, 
  onStart?: () => void, 
  onFinish?: () => void
) => {
  if (onStart) onStart();
  try {
    const response = await fetchWithAuth(url);
    if (!response.ok) {
      let errorMsg = `Download failed (HTTP ${response.status})`;
      try {
        const body = await response.json();
        if (body.detail) errorMsg = body.detail;
      } catch { /* ignore */ }
      throw new Error(errorMsg);
    }
    
    // 1. Try to get pristine filename from Content-Disposition header
    let finalFileName = "";
    try {
      const contentDisposition = response.headers.get('content-disposition');
      if (contentDisposition) {
        const utf8Match = contentDisposition.match(/filename\*=utf-8''([^;]+)/i);
        if (utf8Match && utf8Match[1]) {
          finalFileName = decodeURIComponent(utf8Match[1]);
        } else {
          const normalMatch = contentDisposition.match(/filename="?([^";]+)"?/i);
          if (normalMatch && normalMatch[1]) {
            finalFileName = normalMatch[1];
          }
        }
      }
    } catch (headerErr) {
      console.error("[Download] Header parsing failed:", headerErr);
    }
    
    if (!finalFileName && filename) {
      finalFileName = filename;
    }
    
    if (!finalFileName) {
      try {
        const urlWithoutQuery = url.split('?')[0];
        finalFileName = urlWithoutQuery.split('/').pop() || "download";
      } catch (urlErr) {
        finalFileName = "download";
      }
    }
    
    try {
      finalFileName = decodeURIComponent(finalFileName.replace(/\+/g, ' ')).split('?')[0];
    } catch (cleanErr) {
      console.warn("[Download] Filename cleanup failed, using raw:", finalFileName);
    }
    
    const blob = await response.blob();
    const downloadUrl = window.URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = downloadUrl;
    link.download = finalFileName;
    document.body.appendChild(link);
    link.click();
    
    setTimeout(() => {
      if (document.body.contains(link)) {
        document.body.removeChild(link);
      }
      window.URL.revokeObjectURL(downloadUrl);
    }, 500);
  } catch (error) {
    console.error("Authenticated download error:", error);
    alert("Failed to download file. Please check your connection and login status.");
  } finally {
    if (onFinish) onFinish();
  }
};

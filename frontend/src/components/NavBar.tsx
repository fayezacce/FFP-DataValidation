"use client";

import { useEffect, useState } from "react";
import { useRouter, usePathname } from "next/navigation";
import { isAuthenticated, getUser, clearToken } from "@/lib/auth";
import Link from "next/link";
import { Menu, X, LogOut, User as UserIcon } from "lucide-react";

export default function NavBar() {
  const [user, setLocalUser] = useState<any>(null);
  const [isMenuOpen, setIsMenuOpen] = useState(false);
  const router = useRouter();
  const pathname = usePathname();

  useEffect(() => {
    if (!isAuthenticated() && pathname !== "/login") {
      router.push("/login");
    } else {
      setLocalUser(getUser());
    }
    // Close menu on route change
    setIsMenuOpen(false);
  }, [pathname, router]);

  const handleLogout = () => {
    clearToken();
    router.push("/login");
    setLocalUser(null);
    setIsMenuOpen(false);
  };

  if (pathname === "/login") return null;

  const navLinks = [
    { name: "Upload", href: "/" },
    { name: "Search", href: "/search" },
    { name: "Statistics", href: "/statistics" },
  ];

  if (user && user.role === "admin") {
    navLinks.push({ name: "Admin", href: "/admin" });
  }

  return (
    <nav className="border-b border-[#1e1e20] bg-[#0a0a0b]/80 backdrop-blur-md sticky top-0 z-50">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 h-16 flex items-center justify-between">
        <div className="flex items-center space-x-8">
          <Link href="/" className="text-white font-black text-xl tracking-tighter shrink-0">
            FFP<span className="text-emerald-500">.</span>DATA
          </Link>

          {/* Desktop Links */}
          <div className="hidden md:flex items-center space-x-6">
            {navLinks.map((link) => (
              <Link
                key={link.href}
                href={link.href}
                className={`text-sm font-medium transition-colors ${pathname === link.href ? 'text-white' : 'text-gray-500 hover:text-white'}`}
              >
                {link.name}
              </Link>
            ))}
          </div>
        </div>

        {/* Right side Desktop info */}
        <div className="hidden md:flex items-center space-x-4">
          {user && (
            <div className="flex items-center">
              <span className="text-xs text-gray-500 mr-4">
                Logged in as <span className="text-white font-semibold">{user.username}</span>
              </span>
              <button
                onClick={handleLogout}
                className="text-xs font-bold text-gray-400 hover:text-white transition-colors bg-[#1a1a1c] border border-[#2a2a2e] px-4 py-2 rounded-lg"
              >
                Logout
              </button>
            </div>
          )}
        </div>

        {/* Mobile menu button */}
        <div className="md:hidden flex items-center">
          <button
            onClick={() => setIsMenuOpen(!isMenuOpen)}
            className="text-gray-400 hover:text-white transition-colors p-2"
            aria-label="Toggle menu"
          >
            {isMenuOpen ? <X size={24} /> : <Menu size={24} />}
          </button>
        </div>
      </div>

      {/* Mobile Menu Dropdown */}
      {isMenuOpen && (
        <div className="md:hidden border-t border-[#1e1e20] bg-[#0a0a0b] animate-in slide-in-from-top duration-200">
          <div className="px-4 pt-2 pb-6 space-y-1">
            {navLinks.map((link) => (
              <Link
                key={link.href}
                href={link.href}
                className={`block px-3 py-3 rounded-lg text-base font-medium ${pathname === link.href
                    ? 'bg-[#1a1a1c] text-white'
                    : 'text-gray-400 hover:text-white hover:bg-[#1a1a1c]/50'
                  }`}
              >
                {link.name}
              </Link>
            ))}

            {user && (
              <div className="pt-4 border-t border-[#1e1e20] mt-4">
                <div className="flex items-center px-3 py-3 mb-2">
                  <div className="bg-emerald-500/10 p-2 rounded-full mr-3 text-emerald-500">
                    <UserIcon size={18} />
                  </div>
                  <div>
                    <p className="text-xs text-gray-500">Current User</p>
                    <p className="text-sm font-semibold text-white">{user.username}</p>
                  </div>
                </div>
                <button
                  onClick={handleLogout}
                  className="w-full flex items-center space-x-2 px-3 py-3 rounded-lg text-red-400 hover:bg-red-400/10 transition-colors"
                >
                  <LogOut size={18} />
                  <span className="font-semibold">Logout from Session</span>
                </button>
              </div>
            )}
          </div>
        </div>
      )}
    </nav>
  );
}
